package replication

import (
	"fmt"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/smartystreets-prototypes/go-disruptor"
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
	Handle(*BinlogEvent) error
	Close()
}

type DisruptorBinlogSyncer struct {
	BinlogSyncer
	Handler    ClosableEventHandler
	disruptor  disruptor.Disruptor
	ringBuffer []*DisruptorEvent
}

type DisruptorBinlogSyncerConfig struct {
	BinlogSyncerConfig
	BufferSize       int
	ParseConcurrency int
}

type InitialParseStage struct {
	syncer *DisruptorBinlogSyncer
}
type ConcurrentParseStage struct {
	num             int64
	concurrencyMask int64
	syncer          *DisruptorBinlogSyncer
}
type EventSinkStage struct {
	syncer   *DisruptorBinlogSyncer
	hasError bool
}

func (st InitialParseStage) Consume(lower, upper int64) {
	syncer := st.syncer

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
		if st.syncer.cfg.SemiSyncEnabled && (msg.data[0] == SemiSyncIndicator) {
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
			err := advanceCurrentGtidSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
			if err != nil {
				msg.err = errors.Trace(err)
				continue
			}
		case *MariadbGTIDEvent:
			if syncer.prevGset == nil {
				break
			}
			GTID := event.GTID
			err := advanceCurrentGtidSet(fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber))
			if err != nil {
				msg.err = errors.Trace(err)
				continue
			}
		case *XIDEvent:
			event.GSet = getCurrentGtidSet()
		case *QueryEvent:
			event.GSet = getCurrentGtidSet()
		}

		needStop := false
		select {
		case <-syncer.ctx.Done():
			needStop = true
		}

		if needACK {
			err := syncer.replySemiSyncACK(syncer.nextPos)
			if err != nil {
				msg.err = errors.Trace(err)
				continue
			}
		}

		if needStop {
			msg.err = errors.New("sync is been closing...")
			continue
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
		msg := st.syncer.ringBuffer[lower&DisruptorBufferMask]
		if msg.err != nil {
			continue
		}
		msg.err = st.syncer.parser.FullParse(msg.binEvent, msg.data)
	}
}

func (st EventSinkStage) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		if st.hasError {
			return
		}

		msg := st.syncer.ringBuffer[lower&DisruptorBufferMask]
		if msg.err != nil {
			st.handleError(msg.err)
			return
		}
		err := st.syncer.Handler(msg)
		if err != nil {
			st.handleError(msg.err)
			return
		}
	}
}

func (st EventSinkStage) handleError(err error) {
	st.hasError = true
	st.syncer.cfg.Logger.Errorf("get error, will exit", err)
	st.syncer.Handler.Close()
	st.syncer.Close()
}

func NewDisruptorBinlogSyncer(cfg DisruptorBinlogSyncerConfig, handler ClosableEventHandler) *DisruptorBinlogSyncer {
	ips := InitialParseStage{}
	cpss := make([]disruptor.Consumer, 0)
	ess := EventSinkStage{}
	result := &DisruptorBinlogSyncer{
		BinlogSyncer: *NewBinlogSyncer(cfg.BinlogSyncerConfig),
		Handler:      handler,
		disruptor: disruptor.New(
			disruptor.WithCapacity(DefaultDisruptorBufferSize),
			disruptor.WithConsumerGroup(ips),
			disruptor.WithConsumerGroup(cpss...),
			disruptor.WithConsumerGroup(ess),
		),
	}
	return result
}

func (bs *DisruptorBinlogSyncer) run() {
	defer func() {
		bs.wg.Done()
	}()

	for {
		data, err := bs.c.ReadPacket()
		select {
		case <-bs.ctx.Done():
			return
		default:
		}

		if err != nil {
			bs.cfg.Logger.Error(err)
			return

			// removed the retry logic here
		}

		switch data[0] {
		case OK_HEADER:
			bs.parseEvent(data)
		case ERR_HEADER:
			err = bs.c.HandleErrorPacket(data)
			bs.cfg.Logger.Error(err)
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
