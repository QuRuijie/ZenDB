package db

import (
	"context"
	"fmt"
	"github.com/QuRuijie/zenDB/conf"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/Zentertain/zenlog"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

const (
	// MongoConnPoolLimit is the maximum number of sockets in use in a single server
	// before this session will block waiting for a socket to be available.
	// The default limit is 4096.
	//
	// This limit must be set to cover more than any expected workload of the
	// application. It is a bad practice and an unsupported use case to use the
	// database driver to define the concurrency limit of an application. Prevent
	// such concurrency "at the door" instead, by properly restricting the amount
	// of used resources and number of goroutines before they are created.
	MongoConnPoolLimit = 128
)

const (
	// AdjustDB adjust 回调数据库
	AdjustDB = "adjust"
	// DataBoatDB databoat 默认数据库
	DataBoatDB = "databoat"
)

// AdjustDB 中的 collection.
const (
	InstallCollection = "install"
)

// DataBoatDB 中的 collection.
const (
	// UserTagCollection 存储玩家tag信息 collection 真实名字是 ${project}_UserTagCollection
	UserTagCollectionPostfix = "user_tag"
	// UserProfileCollection 存储玩家Profile信息 collection 真实名字是 ${project}_user_profiles
	UserProfileCollectionPostfix = "user_profiles"
	// UserActCollection 存储玩家action信息 collection 真实名字是 ${project}_user_acts
	UserActCollectionPostfix = "user_acts"

	CampaignInfoPostfix = "campaign_info"

	// 客户端存档冲突，选择存档后备份以前档案
	FilesBackUpCollection = "files_backup"

	// SDKRemoteConfig 功能的 Condition 与 ConditionVal.
	SDKRemConfCondition    = "sdk_rem_conf_conditions"
	SDKRemConfConditionVal = "sdk_rem_conf_condition_vals"

	// config table
	TablesCollections = "tables"
)

const (
	UserActExpireTime = 60 * 24 * 3600
)

var ErrNoDocuments = mongo.ErrNoDocuments

///////////////////////////////////////////////////////////////////////////
// Wrapper Mongo Client
///////////////////////////////////////////////////////////////////////////

type MongoClient struct {
	client *mongo.Client
	dbs    map[string]*mongo.Database
}

func NewMongoClient(opts *conf.MongoConfig) *MongoClient {
	if opts == nil || opts.Addr == "" {
		zenlog.Info("no mongo addr")
		return nil
	}
	mc := new(MongoClient)
	mc.dbs = make(map[string]*mongo.Database)
	client, err := mongo.NewClient(options.Client().SetMaxPoolSize(MongoConnPoolLimit).ApplyURI(opts.Addr))
	if err != nil {
		zenlog.Fatal("create mongo fail:%+v\n", err)
		return nil
	}
	mc.client = client
	ctx, cancelFn := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFn()
	err = client.Connect(ctx)
	if err != nil {
		panic(fmt.Sprintf("mongo connect error: %v\n", err))
	}
	return mc
}

// Database 返回database
func (c *MongoClient) Database(name string) *mongo.Database {
	if db, ok := c.dbs[name]; ok {
		return db
	}

	// create db
	db := c.client.Database(name)
	c.dbs[name] = db

	// create index
	mongoEnsureIndex(db)

	return db
}

// DbColl 返回指定database 的collection
func (c *MongoClient) DbColl(dbName, coll string) *mongo.Collection {
	return c.Database(dbName).Collection(coll)
}

func (c *MongoClient) FindOne(result interface{}, dbName, collName string, query interface{},
	opts ...*options.FindOneOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	findResult := coll.FindOne(context.Background(), query, opts...)
	if findResult.Err() != nil {
		return findResult.Err()
	}

	return findResult.Decode(result)
}

// FindAll
func (c *MongoClient) FindAll(result interface{}, dbName, collName string, query interface{},
	opts ...*options.FindOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	ctx := context.Background()

	cursor, err := coll.Find(ctx, query, opts...)
	if err != nil {
		return err
	}
	return DecodeAll(cursor, ctx, result)
}

func (c *MongoClient) Aggregate(result interface{}, dbName, collName string, pipeline interface{},
	opts ...*options.AggregateOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	ctx := context.Background()

	cursor, err := coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return err
	}

	return DecodeAll(cursor, ctx, result)
}

func (c *MongoClient) FindOneAndUpdate(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.FindOneAndUpdateOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	//ctx context.Context, filter interface{},
	//update interface{}, opts ...*options.FindOneAndUpdateOptions
	result := coll.FindOneAndUpdate(context.Background(), filter, update, opts...)

	return result.Err()
}

func (c *MongoClient) FindOneAndDelete(dbName, collName string, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	result := coll.FindOneAndDelete(context.Background(), filter, opts...)
	return result.Err()
}

// UpdateOne https://docs.mongodb.com/manual/reference/operator/update/
// 注意:最外层需要手动指定操作,如常用的$set: bson.M{"$set":xxxx}
func (c *MongoClient) UpdateOne(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	_, err := coll.UpdateOne(context.Background(), filter, update, opts...)
	return err
}

func (c *MongoClient) UpdateAll(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	_, err := coll.UpdateMany(context.Background(), filter, update)
	return err
}

func (c *MongoClient) UpsertOne(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	upsert := true
	if len(opts) > 0 {
		opts[0].Upsert = &upsert
		return c.UpdateOne(dbName, collName, filter, update, opts...)
	} else {
		opt := options.UpdateOptions{}
		opt.Upsert = &upsert
		return c.UpdateOne(dbName, collName, filter, update, &opt)
	}

}

func (c *MongoClient) InsertOne(dbName, collName string, document interface{},
	opts ...*options.InsertOneOptions) (interface{}, error) {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return nil, fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	result, err := coll.InsertOne(context.Background(), document, opts...)
	if err != nil {
		return nil, err
	}

	return result.InsertedID, nil
}

func (c *MongoClient) InsertMany(dbName, collName string, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return nil, fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	return coll.InsertMany(context.Background(), documents, opts...)
}

func (c *MongoClient) DeleteOne(dbName, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	_, err := coll.DeleteOne(context.Background(), filter, opts...)
	return err
}

func (c *MongoClient) DeleteMany(dbName, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	_, err := coll.DeleteMany(context.Background(), filter, opts...)
	return err
}

func (c *MongoClient) Count(dbName, collName string, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return 0, fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	return coll.CountDocuments(context.Background(), filter, opts...)
}

func (c *MongoClient) BulkWrite(dbName, collName string, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) error {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	_, err := coll.BulkWrite(context.Background(), models, opts...)
	return err
}

///////////////////////////////////////////////////////////////////////////
// Global Mongo Cursor
///////////////////////////////////////////////////////////////////////////
type MyCursor struct {
	*mongo.Cursor
}

func NewMyCursor(c *mongo.Cursor) *MyCursor {
	mc := &MyCursor{Cursor: c}
	return mc
}

func (mc *MyCursor) All(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	ctx := context.Background()

	i := 0
	for mc.Next(ctx) {
		elemp := reflect.New(elemt)
		err := mc.Decode(elemp.Interface())
		if err != nil {
			zenlog.Debug("decode data failed: %+v", err)
			return err
		}
		slicev = reflect.Append(slicev, elemp.Elem())
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return mc.Close(ctx)
}

///////////////////////////////////////////////////////////////////////////
// Global Method
///////////////////////////////////////////////////////////////////////////
// DecodeAll 反射读取所有数据
func DecodeAll(cursor *mongo.Cursor, ctx context.Context, result interface{}) error {
	defer cursor.Close(ctx)

	resultv := reflect.ValueOf(result)

	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()

	for cursor.Next(ctx) {
		elemp := reflect.New(elemt)
		if err := cursor.Decode(elemp.Interface()); err != nil {
			return err
		}

		slicev = reflect.Append(slicev, elemp.Elem())
	}

	resultv.Elem().Set(slicev)

	return nil
}

func NewString(value string) *string {
	r := value
	return &r
}

func NewInt64(value int64) *int64 {
	r := new(int64)
	*r = value
	return r
}

func NewTrue() *bool {
	r := new(bool)
	*r = true
	return r
}

///////////////////////////////////////////////////////////////////////////
// About Database Index
///////////////////////////////////////////////////////////////////////////
func createIndex(c *mongo.Collection, key string, ascending bool, unique bool) {
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	index := makeIndex(key, ascending, unique)
	_, err := c.Indexes().CreateOne(context.Background(), index, opts)
	if err != nil {
		zenlog.Error("create index %s.%s failed, err:%v", c.Name(), key, err)
	} else {
		zenlog.Info("create index %s.%s success", c.Name(), key)
	}
}

func makeIndex(key string, ascending bool, unique bool) mongo.IndexModel {
	var value int
	if ascending {
		value = 1
	} else {
		value = -1
	}

	keys := bsonx.Doc{{Key: key, Value: bsonx.Int32(int32(value))}}
	index := mongo.IndexModel{}
	index.Keys = keys
	index.Options = options.Index()
	index.Options.Unique = &unique
	return index
}

// 新功能的创建索引方式
func ensureIndexKey(db *mongo.Database, collection string, ops *options.IndexOptions, keys ...string) {
	indexes := db.Collection(collection).Indexes()
	index := mongo.IndexModel{}
	kk := bsonx.Doc{}
	indexName := strings.Builder{}
	for _, k := range keys {
		indexName.WriteString(k)
		indexName.WriteString("_1_")
		kk = kk.Append(k, bsonx.Int32(1))
	}
	name := indexName.String()
	if ops == nil {
		ops = &options.IndexOptions{}
	}
	ops.SetBackground(true)
	ops.Name = NewString(name[:len(name)-1])
	index.Options = ops
	index.Keys = kk

	str, err := indexes.CreateOne(context.Background(), index)
	if err != nil {
		zenlog.Error("create index fail,%s %+v", db.Name(), err)
	} else {
		zenlog.Debug("create index,%s %+v", db.Name(), str)
	}
}

func indexWithExpire(db *mongo.Database, collection string, expireTime int32, keys ...string) {
	indexes := db.Collection(collection).Indexes()
	index := mongo.IndexModel{}
	ops := options.IndexOptions{}
	ops.SetBackground(true)
	ops.SetExpireAfterSeconds(expireTime)
	index.Options = &ops

	kk := bson.M{}
	for _, k := range keys {
		kk[k] = 1
	}
	index.Keys = kk

	str, err := indexes.CreateOne(context.Background(), index)
	if err != nil {
		zenlog.Error("create expired index fail,%+v", err)
	} else {
		zenlog.Debug("create expired index,%+v", str)
	}
}

func EnsureExpireIndex(db *mongo.Database, collection string, expireTime int32, keys ...string) {
	indexWithExpire(db, collection, expireTime, keys...)
}

func EnsureUniqIndex(db *mongo.Database, collection string, keys ...string) {
	uniqOps := &options.IndexOptions{}
	uniqOps.SetUnique(true)
	ensureIndexKey(db, collection, uniqOps, keys...)
}

func EnsureIndex(db *mongo.Database, collection string, keys ...string) {
	defaultOps := &options.IndexOptions{}
	ensureIndexKey(db, collection, defaultOps, keys...)
}

// adjustDBEnsureIndex adjust 数据库建立索引
func adjustDBEnsureIndex(db *mongo.Database) {
	coll := db.Collection(InstallCollection)
	createIndex(coll, "adid", true, true)
	createIndex(coll, "app_id", true, false)
}

// 默认数据库建立索引
func defaultDBEnsureIndex(db *mongo.Database) {
	defaultOps := &options.IndexOptions{}
	// 这里可以带上projectId，但是要放到userId等后面
	// 考虑到实际没有那么多相同的情况以及插入速度 去掉了projectId联合索引
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "userId")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "adjustId")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "deviceId")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "idfa")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "idfv")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "androidId")
	ensureIndexKey(db, FilesBackUpCollection, defaultOps, "gpsAdid")

	// 过期类型索引
	//indexWithExpire(db, FilesBackUpCollection, 24*3600*180, "UpdatedAt")
	indexWithExpire(db, FilesBackUpCollection, 0, "expireAt")
}

func mongoEnsureIndex(db *mongo.Database) {
	switch db.Name() {
	case AdjustDB:
		adjustDBEnsureIndex(db)
	case DataBoatDB:
		defaultDBEnsureIndex(db)
	}
}
