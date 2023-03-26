package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

const CONN = "postgres://admin:password@localhost:8888/postgres?replication=database"
const SLOT_NAME = "car_slot"
const OUTPUT_PLUGIN = "pgoutput"

type Update struct {
	NewData []byte
	OldData []byte
}

type Updates map[string]Update

var Event = struct {
	Relation string
	Columns  []string
	relation *pglogrepl.RelationMessage
}{}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	conn, err := pgconn.Connect(ctx, CONN)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	//1. Drop existing PUBLICATION
	if _, err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS carpub;").ReadAll(); err != nil {
		fmt.Printf("failed to drop publication: %v", err)
	}

	//2. Create PUBLICATION
	if _, err := conn.Exec(ctx, "CREATE PUBLICATION carpub FOR TABLE car;").ReadAll(); err != nil {
		fmt.Printf("failed to create publication: %v", err)
	}

	// 3. Create replication slot
	if _, err = pglogrepl.CreateReplicationSlot(ctx, conn, SLOT_NAME, OUTPUT_PLUGIN, pglogrepl.CreateReplicationSlotOptions{Temporary: true}); err != nil {
		fmt.Printf("failed to create a replication slot: %v", err)
	}

	var msgPointer pglogrepl.LSN
	pluginArguments := []string{"proto_version '1'", "publication_names 'carpub'"}

	// 4. Start replication process
	err = pglogrepl.StartReplication(ctx, conn, SLOT_NAME, msgPointer, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		fmt.Printf("failed to establish start replication: %v", err)
	}

	relations := map[uint32]*pglogrepl.RelationMessage{}

	var pingTime time.Time
	for ctx.Err() != context.Canceled {
		if time.Now().After(pingTime) {
			if err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: msgPointer}); err != nil {
				fmt.Printf("failed to send standby update: %v", err)
			}
			pingTime = time.Now().Add(10 * time.Second)
		}

		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		msg, err := conn.ReceiveMessage(ctx)
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			fmt.Printf("something went wrong while listening for message: %v", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				fmt.Println("server: confirmed standby")
			case pglogrepl.XLogDataByteID:
				walLog, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					fmt.Printf("failed to parse logical WAL log: %v", err)
				}

				var msg pglogrepl.Message
				if msg, err = pglogrepl.Parse(walLog.WALData); err != nil {
					fmt.Printf("failed to parse logical replication message: %v", err)
				}
				switch m := msg.(type) {
				case *pglogrepl.RelationMessage:
					Event.Columns = []string{}
					for _, col := range m.Columns {
						Event.Columns = append(Event.Columns, col.Name)
					}
					Event.Relation = m.RelationName
					relations[m.RelationID] = m
					log.Printf("Relation %d %s has %d columns", m.RelationID, m.RelationName, m.ColumnNum)
				case *pglogrepl.InsertMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("INSERT %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("%s: %s ", Event.Columns[i], string(m.Tuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
				case *pglogrepl.UpdateMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("UPDATE %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("New %s: %s ", Event.Columns[i], string(m.NewTuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
					sb.Reset()

					sb.WriteString(fmt.Sprintf("UPDATE %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("Old %s: %s ", Event.Columns[i], string(m.OldTuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())

					rinfo, ok := relations[m.RelationID]
					if !ok {
						log.Fatalf("unknown relation ID %d", m.RelationID)
					}

					// This function can be used to perform transformation
					mapColumns(m.OldTuple.Columns, m.NewTuple.Columns, rinfo)
				case *pglogrepl.DeleteMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("DELETE %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("%s: %s ", Event.Columns[i], string(m.OldTuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
				case *pglogrepl.TruncateMessage:
					fmt.Println("ALL GONE (TRUNCATE)")
				}
			}
		default:
			fmt.Printf("received unexpected message: %T", msg)
		}
	}
}

func mapColumns(old []*pglogrepl.TupleDataColumn, new []*pglogrepl.TupleDataColumn,
	relInfo *pglogrepl.RelationMessage) Updates {
	updates := make(Updates)

	for idx, oldData := range old {
		meta := relInfo.Columns[idx]
		name := meta.Name

		newData := new[idx]
		if !bytes.Equal(newData.Data, oldData.Data) {
			updates[name] = Update{
				NewData: newData.Data,
				OldData: oldData.Data,
			}
		}
	}
	return updates
}
