package replication

import (
	"fmt"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/smartystreets-prototypes/go-disruptor"
)

const (
	DisruptorBufferSize   = 8192
	DisruptorBufferMask   = DisruptorBufferSize - 1
	DisruptorReservations = 1
)

type DisruptorEvent struct {
	data     []byte
	binEvent *BinlogEvent
	err      error
}

type DisruptorBinlogSyncer struct {
	BinlogSyncer
	OnEvent    OnEventFunc
	disruptor  disruptor.Disruptor
	ringBuffer []*DisruptorEvent
}

type InitialParseStage struct {
	syncer *DisruptorBinlogSyncer
}
type ConcurrentParseStage struct {
	ringBuffer []*DisruptorEvent
}
type EventSinkStage struct {
	ringBuffer []*DisruptorEvent
}

func (st InitialParseStage) Consume(lower, upper int64) {
	b := st.syncer
	msg := b.ringBuffer[lower&DisruptorBufferMask]
	//skip OK byte, 0x00
	msg.data = msg.data[1:]

	needACK := false
	if st.syncer.cfg.SemiSyncEnabled && (msg.data[0] == SemiSyncIndicator) {
		needACK = msg.data[1] == 0x01
		//skip semi sync header
		msg.data = msg.data[2:]
	}

	e, body, err := b.parser.QuickParse(msg.data)
	if err != nil {
		msg.err = errors.Trace(err)
		return
	}

	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	}

	getCurrentGtidSet := func() GTIDSet {
		if b.currGset == nil {
			return nil
		}
		return b.currGset.Clone()
	}

	advanceCurrentGtidSet := func(gtid string) error {
		// todo should remove this judgement and set currGset during startup
		if b.currGset == nil {
			b.currGset = b.prevGset.Clone()
		}
		err := b.currGset.Update(gtid)
		return err
	}

	switch event := e.Event.(type) {
	case *RotateEvent:
		b.nextPos.Name = string(event.NextLogName)
		b.nextPos.Pos = uint32(event.Position)
		b.cfg.Logger.Infof("rotate to %s", b.nextPos)
	case *GTIDEvent:
		if b.prevGset == nil {
			break
		}
		u, _ := uuid.FromBytes(event.SID)
		err := advanceCurrentGtidSet(fmt.Sprintf("%s:%d", u.String(), event.GNO))
		if err != nil {
			msg.err = errors.Trace(err)
			return
		}
	case *MariadbGTIDEvent:
		if b.prevGset == nil {
			break
		}
		GTID := event.GTID
		err := advanceCurrentGtidSet(fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber))
		if err != nil {
			msg.err = errors.Trace(err)
			return
		}
	case *XIDEvent:
		event.GSet = getCurrentGtidSet()
	case *QueryEvent:
		event.GSet = getCurrentGtidSet()
	}

	needStop := false
	select {
	case <-b.ctx.Done():
		needStop = true
	}

	if needACK {
		err := b.replySemiSyncACK(b.nextPos)
		if err != nil {
			msg.err = errors.Trace(err)
			return
		}
	}

	if needStop {
		msg.err = errors.New("sync is been closing...")
		return
	}

	msg.binEvent = e
	msg.data = body
	return

}
func (st ConcurrentParseStage) Consume(lower, upper int64) {
	
}
func (st EventSinkStage) Consume(lower, upper int64) {
}

func NewDisruptorBinlogSyncer(cfg BinlogSyncerConfig) *DisruptorBinlogSyncer {
	return &DisruptorBinlogSyncer{
		BinlogSyncer: *NewBinlogSyncer(cfg),
		OnEvent:      nil,
		disruptor: disruptor.New(
			disruptor.WithCapacity(DisruptorBufferSize),
			disruptor.WithConsumerGroup(InitialParseStage{}),
			disruptor.WithConsumerGroup(ConcurrentParseStage{}, ConcurrentParseStage{}, ConcurrentParseStage{}, ConcurrentParseStage{}),
			disruptor.WithConsumerGroup(EventSinkStage{}),
		),
	}
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
