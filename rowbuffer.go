package rowbuffer

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	redis "gopkg.in/redis.v3"
	// "log"
	"strconv"
	"strings"
)

type RowBuffer struct {
	redisConn   *redis.Client
	sqliteConn  *sql.DB
	tableName   string
	colsName    []string
	query       string
	flushBuffer []interface{}
}

func _err(err error) {
	if err != nil {
		panic(err)
	}
}

func ifToStr(i interface{}) string {
	var t string
	switch i.(type) {
	case string:
		t = i.(string)
	default:
		t = strconv.Itoa(i.(int))
	}
	return t
}

func strsToIfs(s []string) []interface{} {
	vals := make([]interface{}, len(s))
	for i, v := range s {
		vals[i] = v
	}
	return vals
}

func (bf *RowBuffer) get(id interface{}) []string { // id int
	vals, _ := bf.redisConn.HVals(bf.tableName + "-" + ifToStr(id)).Result()

	if len(vals) == 0 {
		return bf.fetchFromSql(id)
	}
	return vals
}

func (bf *RowBuffer) fetchFromSql(id interface{}) []string {
	vals := make([]sql.RawBytes, len(bf.colsName))
	scanArgs := make([]interface{}, len(vals))

	for i := range vals {
		scanArgs[i] = &vals[i]
	}

	var (
		rows *sql.Rows
		e    error
	)
	switch id.(type) {
	case string:
		rows, e = bf.sqliteConn.Query(bf.query)
	default:
		rows, e = bf.sqliteConn.Query(bf.query, id.(int))
	}
	_err(e)

	var ret []string
	for rows.Next() {
		rows.Scan(scanArgs...)
		ret = bf.setRedis(id, vals)
	}

	return ret
}

func (bf *RowBuffer) setRedis(key interface{}, vals []sql.RawBytes) []string {
	var (
		k1  = bf.colsName[0]
		v1  = string(vals[0])
		kv  = make([]string, (len(bf.colsName)-1)*2)
		ret = make([]string, len(bf.colsName))
	)
	ret[0] = v1
	for i, v := range vals[1:] {
		kv[i*2] = bf.colsName[i+1]
		kv[i*2+1] = string(v)
		ret[i+1] = string(v)
	}
	bf.redisConn.HMSet(bf.tableName+"-"+ifToStr(key), k1, v1, kv...)
	return ret
}

func (bf *RowBuffer) set(key interface{}, cols []string, vals []interface{}) {
	var kv []string
	for i := 1; i < len(cols); i++ {
		kv = append(kv, cols[i], ifToStr(vals[i]))
	}
	bf.redisConn.HMSet(bf.tableName+"-"+ifToStr(key), cols[0], ifToStr(vals[0]), kv...)
	for _, v := range bf.flushBuffer {
		if v == key {
			return
		}
	}
	bf.flushBuffer = append(bf.flushBuffer, key)
	if len(bf.flushBuffer) >= 10 {
		bf.flush()
	}
}

func (bf *RowBuffer) close() {
	bf.flush()
	bf.redisConn.Close()
	bf.sqliteConn.Close()
}

func (bf *RowBuffer) flush() {
	for _, key := range bf.flushBuffer {
		bf.flushBuffer = bf.flushBuffer[1:]
		vals, e := bf.redisConn.HVals(bf.tableName + "-" + ifToStr(key)).Result()
		_err(e)
		if len(vals) == 0 {
			continue
		}
		bf.updateSql(key, bf.colsName, strsToIfs(vals))
	}
}

func (bf *RowBuffer) updateSql(key interface{}, cols []string, vals []interface{}) {
	sql := "update " + bf.tableName + " set "
	for i, v := range cols {
		cols[i] = v + " = ?"
	}
	sql += strings.Join(cols, ", ")
	switch key.(type) {
	case int:
		sql += " where id = ?"
		vals = append(vals, key)
	}

	_, e := bf.sqliteConn.Exec(sql, vals...)
	_err(e)
}

func (bf *RowBuffer) clearBuffer(key interface{}) {
	bf.redisConn.Expire(bf.tableName+"-"+ifToStr(key), 0)
}

func getRedisConn(addr string, password string, db int64) *redis.Client {
	// addr: "127.0.0.1:6379"
	// password: ""
	// db: 0
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	_, err := client.Ping().Result()
	_err(err)
	return client
}

func getSqliteConn(db string) *sql.DB {
	conn, err := sql.Open("sqlite3", db)
	_err(err)
	return conn
}

func genRowBuffer(r *redis.Client, s *sql.DB, tbName string, colsName []string, query string) *RowBuffer {
	var flushBuffer []interface{}
	return &RowBuffer{r, s, tbName, colsName, query, flushBuffer}
}

/*
func main() {
	bf := genRowBuffer(
		getRedisConn("127.0.0.1:6379", "", 0),
		getSqliteConn("./middle.db"),
		"test",
		[]string{"id", "name", "genre", "career"},
		"select * from test where id = ?",
	)

	bf.clearBuffer(1)
	log.Println(bf.get(1))
	bf.set(1, []string{"name", "genre"}, []interface{}{"Lily", 0})
	log.Println(bf.get(1))
	// bf.flush()
	bf.close()
}
*/
