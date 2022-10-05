package replication

import (
	"fmt"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/smartystreets-prototypes/go-disruptor"
	"time"
)

const (
	DefaultDisruptorBufferSize = 8192
	DefaultParseConcurrency    = 4
	DisruptorBufferMask        = DefaultDisruptorBufferSize - 1
	DisruptorReservations      = 1
)

type DisruptorEvent struct {
	data     []byte
	binEvent *BinlogEvent
	err      error
}

type ClosableEventHandler interface {
	Handle(event *BinlogEvent) error
	HandleError(err error)
	Close()
}

type DisruptorBinlogSyncer struct {
	*BinlogSyncer
	Handler    ClosableEventHandler
	disruptor  disruptor.Disruptor
	ringBuffer []*DisruptorEvent
}

type DisruptorBinlogSyncerConfig struct {
	BinlogSyncerConfig
	BufferSize       int
	ParseConcurrency int
	// Don't manage gtid set inside go-mysql.
	SkipGTIDSetManagement bool
}

type SyncParseStage struct {
	// don't have reference type so use slice instead
	syncer                []*DisruptorBinlogSyncer
	skipGTIDSetManagement bool
}
type ConcurrentParseStage struct {
	num             int64
	concurrencyMask int64
	ringBuffer      []*DisruptorEvent
	parser          *BinlogParser
}
type EventSinkStage struct {
	syncer   []*DisruptorBinlogSyncer
	hasError bool
}

func (st SyncParseStage) Consume(lower, upper int64) {
	syncer := st.syncer[0]

	getCurrentGtidSet := func() GTIDSet {
		if syncer.currGset == nil {
			return nil
		}
		return syncer.currGset.Clone()
	}

	advanceCurrentGtidSet := func(gtid string) error {
		// todo should remove this judgement and set currGset during startup
		if syncer.currGset == nil {
			syncer.currGset = syncer.prevGset.Clone()
		}
		err := syncer.currGset.Update(gtid)
		return err
	}

	for ; lower <= upper; lower++ {
		msg := syncer.ringBuffer[lower&DisruptorBufferMask]
		//skip OK byte, 0x00
		msg.data = msg.data[1:]

		needACK := false
		if syncer.cfg.SemiSyncEnabled && (msg.data[0] == SemiSyncIndicator) {
			needACK = msg.data[1] == 0x01
			//skip semi sync header
			msg.data = msg.data[2:]
		}

		e, body, err := syncer.parser.QuickParse(msg.data)
		if err != nil {
			msg.err = errors.Trace(err)
			continue
		}

		if e.Header.LogPos > 0 {
			// Some events like FormatDescriptionEvent return 0, ignore.
			syncer.nextPos.Pos = e.Header.LogPos
		}

		switch event := e.Event.(type) {
		case *RotateEvent:
			syncer.nextPos.Name = string(event.NextLogName)
			syncer.nextPos.Pos = uint32(event.Position)
			syncer.cfg.Logger.Infof("rotate to %s", syncer.nextPos)
		case *GTIDEvent:
			if syncer.prevGset == nil {
				break
			}
			u, _ := uuid.FromBytes(event.SID)
			if !st.skipGTIDSetManagement {
				err := advanceCurrentGtidSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
				if err != nil {
					msg.err = errors.Trace(err)
					continue
				}
			}
		case *MariadbGTIDEvent:
			if syncer.prevGset == nil {
				break
			}
			GTID := event.GTID
			if !st.skipGTIDSetManagement {
				err := advanceCurrentGtidSet(fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber))
				if err != nil {
					msg.err = errors.Trace(err)
					continue
				}
			}
		case *XIDEvent:
			if !st.skipGTIDSetManagement {
				event.GSet = getCurrentGtidSet()
			}
		case *QueryEvent:
			if !st.skipGTIDSetManagement {
				event.GSet = getCurrentGtidSet()
			}
		}

		if needACK {
			err := syncer.replySemiSyncACK(syncer.nextPos)
			if err != nil {
				msg.err = errors.Trace(err)
				continue
			}
		}

		msg.binEvent = e
		msg.data = body
	}
}

func (st ConcurrentParseStage) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		if lower&st.concurrencyMask != st.num {
			continue
		}
		msg := st.ringBuffer[lower&DisruptorBufferMask]
		if msg.err != nil {
			continue
		}
		msg.err = st.parser.FullParse(msg.binEvent, msg.data)
	}
}

func (st EventSinkStage) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		if st.hasError {
			return
		}

		msg := st.syncer[0].ringBuffer[lower&DisruptorBufferMask]
		if msg.err != nil {
			st.handleError(msg.err)
			return
		}
		err := st.syncer[0].Handler.Handle(msg.binEvent)
		if err != nil {
			st.handleError(err)
			return
		}
	}
}

func (st EventSinkStage) handleError(err error) {
	st.syncer[0].Handler.HandleError(err)
	st.hasError = true
	st.syncer[0].cfg.Logger.Errorf("get error %v, will exit", err)
	st.syncer[0].Handler.Close()
	st.syncer[0].Close()
}

func NewDisruptorBinlogSyncer(cfg DisruptorBinlogSyncerConfig, handler ClosableEventHandler) *DisruptorBinlogSyncer {
	var bufferSize int
	var concurrency int
	if cfg.BufferSize > 0 {
		bufferSize = cfg.BufferSize
	} else {
		bufferSize = DefaultDisruptorBufferSize
	}
	if cfg.ParseConcurrency > 0 {
		concurrency = cfg.ParseConcurrency
	} else {
		concurrency = DefaultParseConcurrency
	}

	syncPlaceholder := make([]*DisruptorBinlogSyncer, 1)
	ips := SyncParseStage{
		syncer:                syncPlaceholder,
		skipGTIDSetManagement: cfg.SkipGTIDSetManagement,
	}
	ess := EventSinkStage{
		syncer: syncPlaceholder,
	}

	rb := make([]*DisruptorEvent, bufferSize)
	bs := NewBinlogSyncer(cfg.BinlogSyncerConfig)
	cps := make([]disruptor.Consumer, 0)
	for i := 0; i < concurrency; i++ {
		each := ConcurrentParseStage{
			num:             int64(i),
			concurrencyMask: int64(concurrency - 1),
			ringBuffer:      rb,
			parser:          bs.parser,
		}
		cps = append(cps, each)
	}

	result := &DisruptorBinlogSyncer{
		BinlogSyncer: bs,
		Handler:      handler,
		disruptor: disruptor.New(
			disruptor.WithCapacity(int64(bufferSize)),
			disruptor.WithConsumerGroup(ips),
			disruptor.WithConsumerGroup(cps...),
			disruptor.WithConsumerGroup(ess),
		),
		ringBuffer: rb,
	}

	syncPlaceholder[0] = result
	return result
}

func informAndClose(handler ClosableEventHandler, err error) {
	handler.HandleError(err)
	handler.Close()
}

func (bs *DisruptorBinlogSyncer) run() {
	defer func() {
		if e := recover(); e != nil {
			bs.cfg.Logger.Errorf("Err: %v\n Stack: %s", e, Pstack())
			informAndClose(bs.Handler, fmt.Errorf("err: %v", e))
		}
		bs.wg.Done()
	}()

	for {
		data, err := bs.c.ReadPacket()
		select {
		case <-bs.ctx.Done():
			bs.Handler.Close()
			return
		default:
		}

		if err != nil {
			bs.cfg.Logger.Error(err)
			informAndClose(bs.Handler, err)
			return

			// removed the retry logic here
		}

		//set read timeout
		if bs.cfg.ReadTimeout > 0 {
			_ = bs.c.SetReadDeadline(time.Now().Add(bs.cfg.ReadTimeout))
		}

		switch data[0] {
		case OK_HEADER:
			bs.parseEvent(data)
		case ERR_HEADER:
			err = bs.c.HandleErrorPacket(data)
			bs.cfg.Logger.Error(err)
			informAndClose(bs.Handler, err)
			return
		case EOF_HEADER:
			// refer to https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
			// when COM_BINLOG_DUMP command use BINLOG_DUMP_NON_BLOCK flag,
			// if there is no more event to send an EOF_Packet instead of blocking the connection
			bs.cfg.Logger.Info("receive EOF packet, no more binlog event now.")
			continue
		default:
			bs.cfg.Logger.Errorf("invalid stream header %c", data[0])
			continue
		}
	}
}

func (bs *DisruptorBinlogSyncer) parseEvent(data []byte) {
	sequence := bs.disruptor.Reserve(DisruptorReservations)
	bs.ringBuffer[sequence&DisruptorBufferMask] = &DisruptorEvent{data: data}
	bs.disruptor.Commit(sequence, sequence)
}

func (bs *DisruptorBinlogSyncer) Close() {
	bs.disruptor.Close()
	bs.BinlogSyncer.Close()
}

// StartSync starts syncing from the `pos` position.
func (bs *DisruptorBinlogSyncer) StartSync(pos Position) error {
	bs.cfg.Logger.Infof("begin to sync binlog from position %s", pos)

	bs.m.Lock()
	defer bs.m.Unlock()

	if bs.running {
		return errors.Trace(errSyncRunning)
	}

	if err := bs.prepareSyncPos(pos); err != nil {
		return errors.Trace(err)
	}

	bs.running = true

	bs.wg.Add(1)
	go bs.run()
	go bs.disruptor.Read()

	return nil
}

// StartSyncGTID starts syncing from the `gset` GTIDSet.
func (bs *DisruptorBinlogSyncer) StartSyncGTID(gset GTIDSet) error {
	bs.cfg.Logger.Infof("begin to sync binlog from GTID set %s", gset)

	bs.prevGset = gset

	bs.m.Lock()
	defer bs.m.Unlock()

	if bs.running {
		return errors.Trace(errSyncRunning)
	}

	// establishing network connection here and will start getting binlog events from "gset + 1", thus until first
	// MariadbGTIDEvent/GTIDEvent event is received - we effectively do not have a "current GTID"
	bs.currGset = nil

	if err := bs.prepare(); err != nil {
		return errors.Trace(err)
	}

	var err error
	switch bs.cfg.Flavor {
	case MariaDBFlavor:
		err = bs.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		// default use MySQL
		err = bs.writeBinlogDumpMysqlGTIDCommand(gset)
	}

	if err != nil {
		return err
	}

	bs.running = true

	bs.wg.Add(1)
	go bs.run()
	go bs.disruptor.Read()
	return nil
}
