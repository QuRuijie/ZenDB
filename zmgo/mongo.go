package zmgo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Zentertain/zenlog"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"io/ioutil"
	"reflect"
	"strings"
	"time"
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

///////////////////////////////////////////////////////////////////////////
// Wrapper Mongo Client
///////////////////////////////////////////////////////////////////////////

type MongoClient struct {
	client *mongo.Client
	dbs    map[string]*mongo.Database
}

// FC You can select your client by your self
// But you must send a function what can return a MongoClient by your message
type FC func(projectId string) (*MongoClient, error)

// FindClient If you want select client by yourself you need assign a value to this variable
var FindClient FC

var findClient FC = func(projectId string) (*MongoClient, error) {
	if FindClient == nil {
		return nil, errors.New("No function to return a client, please execute SetFindClient()!")
	}
	return FindClient(projectId)
}

// NewMongoClient create MongoClient use default MongoConnPoolLimit
func NewMongoClient(uri string) (*MongoClient, error) {
	option := options.Client().ApplyURI(uri).SetMaxPoolSize(MongoConnPoolLimit).SetReadPreference(readpref.Nearest())
	return NewMongoClientWithOption(option)
}

//NewDocumentClient create MongoClient
func NewDocumentClient(uri, CAFile string) (*MongoClient, error) {
	tlsConfig, err := GetCustomTLSConfig(CAFile)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get TLS config fail:%+v\n", err))
	}
	//option := options.Client().ApplyURI(uri).SetMaxPoolSize(MongoConnPoolLimit).SetReadPreference(readpref.Nearest()).SetTLSConfig(tlsConfig)
	option := options.Client().ApplyURI(uri).SetTLSConfig(tlsConfig)
	return NewMongoClientWithOption(option)
}

// NewMongoClientWithOption NewMongoClient create MongoClient by your options.ClientOptions
func NewMongoClientWithOption(option *options.ClientOptions) (*MongoClient, error) {
	client, err := mongo.NewClient(option)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("create mongo fail:%+v\n", err))
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFn()

	err = client.Connect(ctx)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("mongo connect error: %v\n", err))
	}

	//err = client.Ping(context.Background(), readpref.Primary())
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ping mongo fail:%+v", err))
	}

	return &MongoClient{client: client, dbs: make(map[string]*mongo.Database)}, nil
}

// Database 返回database
func (c *MongoClient) Database(name string) *mongo.Database {
	if db, ok := c.dbs[name]; ok {
		return db
	}

	// create db4
	db := c.client.Database(name)
	c.dbs[name] = db

	return db
}

func DB(projectId string, dbname string) *mongo.Database {
	c, err := findClient(projectId)
	if err != nil {
		return nil
	}
	return c.Database(dbname)
}

//SetFindClient If you want select client by yourself you need execute it
func SetFindClient(FindClientFunction FC) {
	FindClient = FindClientFunction
}

// DbColl 返回指定database 的collection
func (c *MongoClient) DbColl(dbName, coll string) *mongo.Collection {
	return c.Database(dbName).Collection(coll)
}

// Collection 返回对应项目下的Collection
func Collection(projectId, collection string) *mongo.Collection {
	c, err := findClient(projectId)
	if err == nil {
		return c.DbColl(projectId, collection)
	}
	return nil
}

func FindOne(result interface{}, proj string, collName string, query interface{},
	opts ...*options.FindOneOptions) error {
	c, err := findClient(proj)
	if err != nil {
		return err
	}
	return c.FindOne(result, proj, collName, query, opts...)
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

func FindAll(result interface{}, dbName, collName string, query interface{},
	opts ...*options.FindOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.FindAll(result, dbName, collName, query, opts...)
}

func (c *MongoClient) FindAll(result interface{}, dbName, collName string, query interface{},
	opts ...*options.FindOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		return errors.New("result argument must be a slice address")
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

func Aggregate(result interface{}, dbName, collName string, pipeline interface{},
	opts ...*options.AggregateOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.Aggregate(result, dbName, collName, pipeline, opts...)
}

func (c *MongoClient) Aggregate(result interface{}, dbName, collName string, pipeline interface{},
	opts ...*options.AggregateOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		return errors.New("result argument must be a slice address")
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

func FindOneAndUpdate(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.FindOneAndUpdateOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.FindOneAndUpdate(dbName, collName, filter, update, opts...)
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

func FindOneAndDelete(dbName, collName string, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.FindOneAndDelete(dbName, collName, filter, opts...)
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
func UpdateOne(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.UpdateOne(dbName, collName, filter, update, opts...)
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

func UpdateAll(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.UpdateAll(dbName, collName, filter, update, opts...)
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

func UpsertOne(dbName, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.UpsertOne(dbName, collName, filter, update, opts...)
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

func InsertOne(dbName, collName string, document interface{},
	opts ...*options.InsertOneOptions) (interface{}, error) {
	c, err := findClient(dbName)
	if err != nil {
		return nil, err
	}
	return c.InsertOne(dbName, collName, document, opts...)
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

func InsertMany(dbName, collName string, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	c, err := findClient(dbName)
	if err != nil {
		return nil, err
	}
	return c.InsertMany(dbName, collName, documents, opts...)
}

func (c *MongoClient) InsertMany(dbName, collName string, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return nil, fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	return coll.InsertMany(context.Background(), documents, opts...)
}

func DeleteOne(dbName, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.DeleteOne(dbName, collName, filter, opts...)
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

func DeleteMany(dbName, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.DeleteMany(dbName, collName, filter, opts...)
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

func Count(dbName, collName string, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	c, err := findClient(dbName)
	if err != nil {
		return -1, err
	}
	return c.Count(dbName, collName, filter, opts...)
}

func (c *MongoClient) Count(dbName, collName string, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	coll := c.DbColl(dbName, collName)
	if coll == nil {
		return 0, fmt.Errorf("cannot find collection:%+v,%+v", dbName, collName)
	}

	result := make([]interface{}, 0)
	err := c.FindAll(&result, dbName, collName, filter)
	if err != nil {
		return 0, fmt.Errorf("FindAll err: %v", err)
	}
	return int64(len(result)), err
	//return coll.CountDocuments(context.Background(), filter, opts...)
}

func BulkWrite(dbName, collName string, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) error {
	c, err := findClient(dbName)
	if err != nil {
		return err
	}
	return c.BulkWrite(dbName, collName, models, opts...)
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
		return errors.New("result argument must be a slice address")
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

// CollectionFromCommon 从Common数据库中获得Collection
func CollectionFromCommon(proj string, collName string) *mongo.Collection {
	return Collection(proj, collName)
}

// GetCustomTLSConfig 获取TLS证书
func GetCustomTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := new(tls.Config)
	tlsConfig.InsecureSkipVerify = true
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
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
