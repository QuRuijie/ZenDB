package zmgo

import (
	"fmt"
	"github.com/Zentertain/zenlog"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/net/context"
)

var gCommonClient *Client
var gProjectClientMap = make(map[string]*Client)

var ErrNoDocuments = mongo.ErrNoDocuments

const (
	MONGO_DB_CONN_POOL      = 128
	MONGO_FIND_MAX_COUNT    = 100
	MONGO_DB_COMMON         = "common"
	MONGO_GUILD_CHAT_EXPIRE = 7 * 24 * 3600

	COL_GUILD_CONFIG         = "guild_configs"               // 每个项目的配置信息
	COL_GUILD_PROJECT_CONFIG = "guild_project_configs"       // 每个项目个性化的工会配置信息
	COL_GUILD_TABLES         = "guild_table"                 // 工会的动态表 (可能没用，先写上)
	COL_GUILD_INFO           = "guild_info"                  // 工会信息表
	COL_GUILD_USERINFO       = "guild_userinfo"              // 玩家基本信息表(放一些不常改变的数据)
	COL_GUILD_CHATINFO       = "guild_chatinfo"              // 工会聊天信息存储表
	COL_GUILD_AWARD          = "guild_award"                 // 工会互助奖励信息存储表
	COL_GUILD_UID_MAPPING    = "guild_user_id_mapping"       // 新老userId的映射存档
	COL_GUILD_FILE           = "guild_file"                  // 工会存档
	COL_GRPC_HOST_MAPPING    = "grpc_host_mapping"           // 工会id和grpc-chat-host的映射。这玩意放common里吧
	COL_USER_INVITATION      = "guild_user_invitation"       // 玩家收到的邀请函存档
	COL_USER_BEHAVIOR        = "guild_user_behavior"         // 玩家行为的记录
	COL_USER_BEHAVIOR_BI     = "guild_user_behavior_from_bi" // 玩家行为的记录 (通过BI)
	COL_GUILD_REC_ABTEST     = "guild_recommend_abtest"      // 工会推荐ABTest配置表
	COL_ADMIN_RECORD         = "admin_record"                // GM操作记录
)

func Init(addrs map[string]string) {
	for key, uri := range addrs {
		client, err := NewClient(uri)
		if err != nil {
			panic(err)
		}

		if key == "default" {
			gCommonClient = client
		} else {
			gProjectClientMap[key] = client
		}
	}
	zenlog.Debug("project mongo client:%+v", gProjectClientMap)
}

func TestFind() {
	addrs := make(map[string]string)
	addrs["default"] = "mongodb://worker-alpha:D9FkmXcMNUiXbgs4@commgame-alpha-shard-00-00-bj91f.mongodb.net:27016,commgame-alpha-shard-00-01-bj91f.mongodb.net:27016,commgame-alpha-shard-00-02-bj91f.mongodb.net:27016,commgame-alpha-shard-01-00-bj91f.mongodb.net:27016,commgame-alpha-shard-01-01-bj91f.mongodb.net:27016,commgame-alpha-shard-01-02-bj91f.mongodb.net:27016/test?ssl=true&authSource=admin"
	Init(addrs)
	proj := "home_design"

	groupTables := make([]interface{}, 0)
	err := FindAll(&groupTables, proj, "tables", bson.M{})
	if err != nil {
		zenlog.Error("mongo findAll with error:%+v", err)
		return
	}
	fmt.Printf("tables:%+v\n", groupTables)

	// Test find user
	var file interface{}
	err = FindOne(&file, proj, "files", bson.M{"_id": "-aKzMqoiR-1"})
	if err != nil {
		zenlog.Error("mongo find one error :%+v", err)
	}
	fmt.Printf("file:%+v\n", file)
}

func NewClient(uri string) (*Client, error) {
	zenlog.Info("try connect mongo:%+v", uri)

	option := options.Client().ApplyURI(uri)
	option.SetMaxPoolSize(MONGO_DB_CONN_POOL)
	option.ReadPreference = readpref.Nearest()

	client, err := mongo.NewClient(option)
	if err != nil {
		zenlog.Error("create mongo fail:%+v\n", err)
		return nil, err
	}

	err = client.Connect(context.Background())
	if err != nil {
		zenlog.Error("connect mongo fail:%+v", err)
		return nil, err
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		zenlog.Error("ping mongo fail:%+v", err)
	} else {
		zenlog.Info("connect mongo succeed")
	}

	return &Client{client: client, dbs: make(map[string]*mongo.Database)}, nil
}

///////////////////////////////////////////////////////////////////////////
// Wrapper Mongo Client
///////////////////////////////////////////////////////////////////////////
type Client struct {
	client *mongo.Client
	dbs    map[string]*mongo.Database
}

func (c *Client) Database(name string) *mongo.Database {
	if db, ok := c.dbs[name]; ok {
		return db
	}

	// create db
	db := c.client.Database(name)
	c.dbs[name] = db

	// create index
	ensureIndex(db)

	return db
}

func ensureIndexKey(db *mongo.Database, collection string, keys ...string) {
	indexes := db.Collection(collection).Indexes()
	index := mongo.IndexModel{}
	kk := bsonx.Doc{}
	indexName := strings.Builder{}
	for _, k := range keys {
		indexName.WriteString(k)
		indexName.WriteString("_1_")
		kk = kk.Append(k, bsonx.Int32(1))
	}
	ops := options.IndexOptions{}
	ops.SetBackground(true)
	ops.Name = NewString(indexName.String())
	index.Options = &ops
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
		zenlog.Error("create index fail,%+v", err)
	} else {
		zenlog.Debug("create index,%+v", str)
	}
}

func ensureIndex(db *mongo.Database) {
	ensureIndexKey(db, COL_GUILD_USERINFO, "guildId", "invitationStatus")
	ensureIndexKey(db, COL_GUILD_CHATINFO, "_id", "chat.globalSeqId")
	ensureIndexKey(db, COL_GUILD_CHATINFO, "_id", "chat.msgType", "chat.userId")
	ensureIndexKey(db, COL_GUILD_AWARD, "userId", "createTime")
	ensureIndexKey(db, COL_GUILD_INFO, "category", "allowJoin", "minJoinLevel", "memberCount", "createTime") // 推荐工会接口
	ensureIndexKey(db, COL_GUILD_INFO, "category", "extra.robotTypeId", "extra.endTime")
	ensureIndexKey(db, COL_GUILD_INFO, "_id", "category", "memberCount") // 搜索工会接口
	ensureIndexKey(db, COL_GUILD_INFO, "name", "memberCount")            // 搜索工会接口
	ensureIndexKey(db, COL_USER_INVITATION, "guildId", "expireTime")
	ensureIndexKey(db, COL_USER_INVITATION, "userId", "expireTime")
	ensureIndexKey(db, COL_USER_INVITATION, "guildId", "userId")
	ensureIndexKey(db, COL_GUILD_UID_MAPPING, "originUserId")
	ensureIndexKey(db, COL_USER_BEHAVIOR, "userId", "category", "date")
	ensureIndexKey(db, COL_USER_BEHAVIOR_BI, "userId", "category", "date")

	indexWithExpire(db, COL_GUILD_CHATINFO, MONGO_GUILD_CHAT_EXPIRE, "updateTime")
	indexWithExpire(db, COL_GUILD_INFO, 0, "deleteTime")
	indexWithExpire(db, COL_GUILD_AWARD, 3*24*3600, "receiveTime") // 领奖3天后过期
	indexWithExpire(db, COL_USER_INVITATION, 0, "expireTime")
	indexWithExpire(db, COL_USER_BEHAVIOR, 0, "expireTime")
	indexWithExpire(db, COL_USER_BEHAVIOR_BI, 0, "expireTime")
}

///////////////////////////////////////////////////////////////////////////
// Global Method
///////////////////////////////////////////////////////////////////////////
func DB(projectId string, dbname string) *mongo.Database {
	var client *Client
	if c, ok := gProjectClientMap[projectId]; ok {
		//zenlog.Debug("use project mongo client:%+v", projectId)
		client = c
	} else {
		client = gCommonClient
	}

	return client.Database(dbname)
}

// Collection 返回对应项目下的Collection
func Collection(projectId, collection string) *mongo.Collection {
	db := DB(projectId, projectId)
	if db != nil {
		return db.Collection(collection)
	}

	return nil
}

// CollectionFromCommon 从Common数据库中获得Collection
func CollectionFromCommon(proj string, collName string) *mongo.Collection {
	return gCommonClient.Database(proj).Collection(collName)
}

// CommonCollection 返回common Collection
func CommonCollection(name string) *mongo.Collection {
	if gCommonClient != nil {
		return gCommonClient.Database(MONGO_DB_COMMON).Collection(name)
	}

	return nil
}

// DecodeAll 反射读取所有数据
func DecodeAll(cursor *mongo.Cursor, ctx context.Context, result interface{}) error {
	//defer func() {
	//	err := cursor.Close(ctx)
	//	if err != nil {
	//		zenlog.Error("mongo cursor close with error :%+v", err)
	//	}
	//}()
	// mongo cursor close with error :(BadValue) Must specify at least one cursor id in: { killCursors: "messages", cursors: [], $clusterTime: { clusterTime: Timestamp(1579180400, 1), signature: { hash: BinData(0, 6D9FD535E496D4113E33A11A1D9ED0896BCA3EB4), keyId: 6748855865106759681 } }, $db: "5b9f51a14e6581551c43af20_home_design", $readPreference: { mode: "primaryPreferred" } }
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

func FindOne(result interface{}, proj string, collName string, query interface{},
	opts ...*options.FindOneOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	findResult := coll.FindOne(context.Background(), query, opts...)
	if findResult.Err() != nil {
		return findResult.Err()
	}

	return findResult.Decode(result)
}

// FindAll
func FindAll(result interface{}, proj string, collName string, query interface{},
	opts ...*options.FindOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	ctx := context.Background()

	cursor, err := coll.Find(ctx, query, opts...)
	if err != nil {
		return err
	}

	return DecodeAll(cursor, ctx, result)
}

func Aggregate(result interface{}, proj string, collName string, pipeline interface{},
	opts ...*options.AggregateOptions) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	ctx := context.Background()

	cursor, err := coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return err
	}

	return DecodeAll(cursor, ctx, result)
}

func FindOneAndUpdate(result interface{}, proj string, collName string, filter interface{}, update interface{},
	opts ...*options.FindOneAndUpdateOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	//ctx context.Context, filter interface{},
	//update interface{}, opts ...*options.FindOneAndUpdateOptions
	cmdResult := coll.FindOneAndUpdate(context.Background(), filter, update, opts...)

	return cmdResult.Decode(result)
}

func FindOneAndDelete(result interface{}, proj string, collName string, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	cmdResult := coll.FindOneAndDelete(context.Background(), filter, opts...)
	return cmdResult.Decode(result)
}

// UpdateOne https://docs.mongodb.com/manual/reference/operator/update/
// 注意:最外层需要手动指定操作,如常用的$set: bson.M{"$set":xxxx}
func UpdateOne(proj string, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	_, err := coll.UpdateOne(context.Background(), filter, update, opts...)
	return err
}

func UpdateAll(proj string, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	_, err := coll.UpdateMany(context.Background(), filter, update)
	return err
}

func UpsertOne(proj string, collName string, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) error {
	upsert := true
	if len(opts) > 0 {
		opts[0].Upsert = &upsert
		return UpdateOne(proj, collName, filter, update, opts...)
	} else {
		opt := options.UpdateOptions{}
		opt.Upsert = &upsert
		return UpdateOne(proj, collName, filter, update, &opt)
	}

}

func InsertOne(proj string, collName string, document interface{},
	opts ...*options.InsertOneOptions) (interface{}, error) {
	coll := Collection(proj, collName)
	if coll == nil {
		return nil, fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	result, err := coll.InsertOne(context.Background(), document, opts...)
	if err != nil {
		return nil, err
	}

	return result.InsertedID, nil
}

func InsertMany(proj string, collName string, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	coll := Collection(proj, collName)
	if coll == nil {
		return nil, fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	return coll.InsertMany(context.Background(), documents, opts...)
}

func DeleteOne(proj string, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	_, err := coll.DeleteOne(context.Background(), filter, opts...)
	return err
}

func DeleteMany(proj string, collName string, filter interface{},
	opts ...*options.DeleteOptions) error {
	coll := Collection(proj, collName)
	if coll == nil {
		return fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	_, err := coll.DeleteMany(context.Background(), filter, opts...)
	return err
}

func Count(proj string, collName string, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	coll := Collection(proj, collName)
	if coll == nil {
		return 0, fmt.Errorf("cannot find collection:%+v,%+v", proj, collName)
	}

	return coll.CountDocuments(context.Background(), filter, opts...)
}

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
			fmt.Printf("decode data failed: %+v", err)
			return err
		}
		slicev = reflect.Append(slicev, elemp.Elem())
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return mc.Close(ctx)
}
