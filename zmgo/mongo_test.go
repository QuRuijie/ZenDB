package zmgo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestBatchQuery(t *testing.T) {
	//uri := "mongodb://worker-alpha:D9FkmXcMNUiXbgs4@commgame-alpha-shard-00-00-bj91f.mongodb.net:27016,commgame-alpha-shard-00-01-bj91f.mongodb.net:27016,commgame-alpha-shard-00-02-bj91f.mongodb.net:27016,commgame-alpha-shard-01-00-bj91f.mongodb.net:27016,commgame-alpha-shard-01-01-bj91f.mongodb.net:27016,commgame-alpha-shard-01-02-bj91f.mongodb.net:27016/test?ssl=true&authSource=admin"
	////uri := "mongodb://worker-beta:1hyFED1iKyLdS6cf@homedesign-beta-shard-00-00-ipdl7.mongodb.net:27016,homedesign-beta-shard-00-01-ipdl7.mongodb.net:27016,homedesign-beta-shard-00-02-ipdl7.mongodb.net:27016,homedesign-beta-shard-01-00-ipdl7.mongodb.net:27016,homedesign-beta-shard-01-01-ipdl7.mongodb.net:27016,homedesign-beta-shard-01-02-ipdl7.mongodb.net:27016/test?ssl=true&authSource=admin"
	//client := NewClient(uri)
	//db := client.Database("common")
	//
	//proj := "word_connect_v4"
	//result := db.Collection("configs").FindOne(context.Background(), bson.M{"_id": proj})
	//if result.Err() != nil {
	//	zenlog.Error("%+v", result.Err())
	//	return
	//}
	////config := &protocol.AdminProjectConfig{}
	//config := make(map[string]interface{})
	////config := &bson.M{}
	//err := result.Decode(&config)
	//if err != nil {
	//	zenlog.Error("%+v", err)
	//	return
	//}
	//zenlog.Debug("load config,%+v,%+v", proj, config)

	//query := bson.M{
	//	"_id": bson.M{"$in":[]}
	//}
	//uids := []string{"YxJy47FSHWz", "045nCgZgHWz", "1KDn3GWWR", "XTYOVVQkNWk", "Tda4pbWZR", "w1fX83ZWg", "c1NrSLRZR", "WkIKYBWZg", "jqNwRTWZR", "csDOfZMZR", "Mxvp_1WZg", "yLQOhFmWR", "g2evkCiZR", "om5ligWZR", "bVlEx5kZg", "raT5mdkZR", "1rTdddkZg", "EfRr35kWR", "dHWP6piWg", "xvyMz8Kig"}
	//uids := []string{"edaXwY0mR"}
	//fileids := bson.A{}
	//for _, uid := range uids {
	//	fileids = append(fileids, uid+"-1")
	//}
	//
	//query := bson.M{
	//	"_id": bson.M{"$in": fileids},
	//}
	//
	//projection := bson.D{
	//	{"_id", 1},
	//	{"userId", 1},
	//	{"data._userId", 1},
	//	{"data._facebookId", 1},
	//	{"data._userName", 1},
	//	{"data._level", 1},
	//	{"data._ownedHouses.newestHouseId", 1},
	//	{"data._ownedHouses.newestRoomId", 1},
	//	{"data._ownedHouses", 1},
	//	{"data._userCheatCount", 1},
	//	//{"data._userName", 1},
	//	//{"data._facebookName", 1},
	//	//{"data._userAvatarUrl", 1},
	//}
	//
	//cursor, err := db.Collection("files").Find(nil, query, options.Find().SetProjection(projection))
	//if err != nil {
	//	t.Errorf("%+v", err)
	//} else {
	//	results := []map[string]interface{}{}
	//	if err := DecodeAll(cursor, nil, &results); err != nil {
	//		t.Errorf("%+v", err)
	//	} else {
	//		//t.Logf("results:%+v", results)
	//		for _, v := range results {
	//			dd, _ := json.MarshalIndent(v, "", "\t")
	//			t.Logf("%s", dd)
	//			//t.Logf("aa:%+v", v["data"].(map[string]interface{}))
	//			//t.Logf("user:%+v", v)
	//		}
	//	}
	//}
}

func TestIndexWithExpire(t *testing.T) {

	type UserInfo struct {
		ID        string    `bson:"_id"`       // 用户ID
		Tags      []*bson.D `bson:"tags"`      // 用来推荐的标签数据
		UpdatedAt time.Time `bson:"updatedAt"` // 更新时间
	}

	dbName := "test"
	colName := "col_test_expire"
	uri := "mongodb://127.0.0.1:27017"
	cli, err := NewClient(uri)
	if err != nil {
		t.Fatal("new client err", err)
	}
	db := cli.Database(dbName)
	if err != nil {
		t.Fatal("new db err", err)
	}
	col := db.Collection(colName)
	if col == nil {
		t.Fatal("new col err", err)
	}

	// 设置过期key
	indexWithExpire(db, colName, 60, "updatedAt")
	saveUser := &UserInfo{
		ID:        "1",
		Tags:      nil,
		UpdatedAt: time.Now(),
		//UpdatedAt: time.Now().Add(-60 * time.Second)
	}
	opt := options.UpdateOptions{}
	opt.SetUpsert(true)
	_, err = col.UpdateOne(context.Background(), bson.M{"_id": "1"}, bson.M{"$set": &saveUser}, &opt)
	if err != nil {
		t.Fatal("upsert err", err)
	}

	r := col.FindOne(context.Background(), bson.M{"_id": "1"})
	getUser := &UserInfo{}
	r.Decode(getUser)
	t.Log(getUser)
}

func TestObjectId(t *testing.T) {
	type NotifyModel struct {
		ID            primitive.ObjectID `bson:"_id"`           // 生成的通知唯一ID
		UpdatedAt     time.Time          `bson:"updatedAt"`     // 更新时间，过期key
		NotifyType    int32              `bson:"notifyType"`    // 通知类型
		Sender        string             `bson:"sender"`        // 发起通知玩家ID
		Receiver      string             `bson:"receiver"`      // 接收通知玩家ID
		IdempotentKey string             `bson:"idempotentKey"` // 用来处理今日是否发送过类型请求的(type + sender_uid + segment_key)
	}

	dbName := "test"
	colName := "col_test_notify"
	uri := "mongodb://127.0.0.1:27017"
	cli, err := NewClient(uri)
	if err != nil {
		t.Fatal("new client err", err)
	}
	db := cli.Database(dbName)
	if err != nil {
		t.Fatal("new db err", err)
	}
	col := db.Collection(colName)
	if col == nil {
		t.Fatal("new col err", err)
	}

	opt := options.UpdateOptions{}
	opt.SetUpsert(true)

	model := NotifyModel{
		ID:            primitive.NewObjectID(),
		UpdatedAt:     time.Now(),
		NotifyType:    int32(1),
		Sender:        "sender",
		Receiver:      "receiver",
		IdempotentKey: "20200223",
	}

	insertResult, err := col.InsertOne(context.Background(), model)
	if err != nil {
		t.Fatal("upsert err", err)
	}
	t.Log(insertResult)

	//obj, err := primitive.ObjectIDFromHex("5ea27a378a6cb8578b673410")
	//if err != nil {
	//	t.Log(err)
	//}
	r := col.FindOne(context.Background(), bson.M{"_id": insertResult.InsertedID})
	t.Log(r)
	findModel := &NotifyModel{}
	r.Decode(findModel)
	t.Log(findModel)

	deleteResult, err := col.DeleteOne(context.Background(), bson.M{"_id": findModel.ID})
	if err != nil {
		t.Fatal("delete one err", err)
	}

	t.Log(deleteResult.DeletedCount)

	_, err = col.DeleteOne(context.Background(), bson.M{"_id": findModel.ID})
	if err != nil {
		t.Fatal("delete one err", err)
	}
}
