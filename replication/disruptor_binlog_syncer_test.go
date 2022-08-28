package replication

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	. "github.com/pingcap/check"
	"sync"
	"testing"
	"time"
)

func TestDisruptorBinlogSyncer(t *testing.T) {
	TestingT(t)
}

type testEventHandler struct {
	events []*BinlogEvent
}

func (h *testEventHandler) Handle(e *BinlogEvent) error {
	h.events = append(h.events, e)
	fmt.Println(e)
	return nil
}

func (h *testEventHandler) Close() {
}

type testDisruptorSyncerSuite struct {
	b *DisruptorBinlogSyncer
	c *client.Conn

	wg sync.WaitGroup
}

var _ = Suite(&testDisruptorSyncerSuite{})

func (t *testDisruptorSyncerSuite) SetUpSuite(c *C) {
}

func (t *testDisruptorSyncerSuite) TearDownSuite(c *C) {
}

func (t *testDisruptorSyncerSuite) SetUpTest(c *C) {
	var port uint16 = 3306
	var err error

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, port), "root", "root", "")
	if err != nil {
		c.Skip(err.Error())
	}

	_, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test_db")
	c.Assert(err, IsNil)

	_, err = t.c.Execute("USE test_db")
	c.Assert(err, IsNil)

	if t.b != nil {
		t.b.Close()
	}

	cfg := DisruptorBinlogSyncerConfig{
		BinlogSyncerConfig: BinlogSyncerConfig{
			ServerID:   100,
			Host:       *testHost,
			Port:       port,
			User:       "root",
			Password:   "root",
			UseDecimal: true,
		},
		BufferSize:       8192,
		ParseConcurrency: 4,
	}

	t.b = NewDisruptorBinlogSyncer(cfg, &testEventHandler{events: []*BinlogEvent{}})

}

func (t *testDisruptorSyncerSuite) TearDownTest(c *C) {
	if t.b != nil {
		t.b.Close()
		t.b = nil
	}

	if t.c != nil {
		t.c.Close()
		t.c = nil
	}
}

func (t *testDisruptorSyncerSuite) testExecute(c *C, query string) {
	_, err := t.c.Execute(query)
	c.Assert(err, IsNil)
}

func (t *testDisruptorSyncerSuite) createTestMysqlBinlogSyncData(c *C) {
	records := []string{
		"insert into test_tab values (1,'Tom',20.5)",
		"insert into test_tab values (2,'Jerry',100.1)",
		"update test_tab set price=55.5 where id=2",
		"insert into test_tab values (3,'Page',33.3)",
		"update test_tab set price=78.13 where id=1",
	}
	for _, record := range records {
		t.testExecute(c, record)
	}
}

func (t *testDisruptorSyncerSuite) TestMysqlBinlogSync(c *C) {

	str := `DROP TABLE IF EXISTS test_tab`
	t.testExecute(c, str)

	str = `CREATE TABLE test_tab (
			id BIGINT(64) UNSIGNED,
			name VARCHAR(256),
			price DOUBLE,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	t.testExecute(c, str)

	//use row format
	t.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	r, err := t.c.Execute("SELECT @@gtid_mode")
	c.Assert(err, IsNil)
	modeOn, _ := r.GetString(0, 0)
	if modeOn != "ON" {
		c.Skip("GTID mode is not ON")
	}

	r, err = t.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'")
	c.Assert(err, IsNil)

	var masterUuid uuid.UUID
	if s, _ := r.GetString(0, 1); len(s) > 0 && s != "NONE" {
		masterUuid, err = uuid.Parse(s)
		c.Assert(err, IsNil)
	}

	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

	err = t.b.StartSyncGTID(set)
	c.Assert(err, IsNil)

	t.createTestMysqlBinlogSyncData(c)

	time.Sleep(100 * time.Minute)
}
