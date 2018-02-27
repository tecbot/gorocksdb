package main

import (
	"github.com/leeyazhou/gorocksdb"
	"log"
	"errors"
	"strconv"
)

const (
	DB_PATH = "/tmp/gorocksdb"
)

func main() {
	db, err := OpenDB()
	if err != nil {
		log.Println("fail to open db,", nil, db)
	}

	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(true)

	writeOptions := gorocksdb.NewDefaultWriteOptions()
	writeOptions.SetSync(true)

	for i := 0; i < 10000; i++ {
		keyStr := "aa" + strconv.Itoa(i)
		var key []byte = []byte(keyStr)
		db.Put(writeOptions, key, key)
		log.Println(i, keyStr)
		slice, err2 := db.Get(readOptions, key)
		if err2 != nil {
			log.Println("获取数据异常：", key, err2)
			continue
		}
		log.Println("获取数据：", slice.Size(), string(slice.Data()))
	}

	//defer readOptions.Destroy()
	//defer writeOptions.Destroy()
}

func OpenDB() (*gorocksdb.DB, error) {
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)

	bloomFilter := gorocksdb.NewBloomFilter(10)

	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(false)

	rateLimiter := gorocksdb.NewRateLimiter(10000000, 10000, 10)
	options.SetRateLimiter(rateLimiter)
	options.SetCreateIfMissing(true)
	options.EnableStatistics()
	options.SetWriteBufferSize(8 * gorocksdb.KB)
	options.SetMaxWriteBufferNumber(3)
	options.SetMaxBackgroundCompactions(10)
	options.SetCompression(gorocksdb.SnappyCompression)
	options.SetCompactionStyle(gorocksdb.UniversalCompactionStyle)

	options.SetHashSkipListRep(2000000, 4, 4)

	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockBasedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(64 * gorocksdb.KB))
	blockBasedTableOptions.SetFilterPolicy(bloomFilter)
	blockBasedTableOptions.SetBlockSizeDeviation(5)
	blockBasedTableOptions.SetBlockRestartInterval(10)
	blockBasedTableOptions.SetBlockCacheCompressed(gorocksdb.NewLRUCache(64 * gorocksdb.KB))
	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
	blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)

	options.SetBlockBasedTableFactory(blockBasedTableOptions)
	//log.Println(bloomFilter, readOptions)
	options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))

	options.SetAllowConcurrentMemtableWrites(false)

	db, err := gorocksdb.OpenDb(options, DB_PATH)

	if err != nil {
		log.Fatalln("OPEN DB error", db, err)
		db.Close()
		return nil, errors.New("fail to open db")
	} else {
		log.Println("OPEN DB success", db)
	}
	return db, nil
}
