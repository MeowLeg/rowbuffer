rowbuffer其实算是造轮子了，主要是因为之前的项目大量使用的是Sqlite3嵌入式数据库，在大量访问时速度难以令人满意。
明年的预算里，单列了几台缓存的服务器，用于部署redis，以减轻系统IO的压力。

这个rowbuffer主要由4个API

1、func (bf *RowBuffer) get(key []interface) []string
该函数用于从redis获取数据，当获取不到时会从数据库直接读，同时写入redis

2、func (bf *RowBuffer) set(key []interface, cols []string, vals []interface{})
该函数用户更新行数据到redis中，当累计更新超过10个时，回写到数据库中

3、func (bf *RowBuffer) clearBuffer(key []interface)
该函数用于清除redis中的键值

4、func (bf *RowBuffer) flush()
该函数用于回写redis的更新数据到数据库中

5、func (bf *RowBuffer) close()
回写redis的数据到数据库，之后关闭redis和数据库的连接
