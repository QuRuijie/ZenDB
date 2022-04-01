package zmgo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"sort"
	"testing"
	"time"
)

const (
	URI     = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	URI_DOC = "mongodb://root:PuPgX7d2jBQ5@TestDocumentDBScale-136593183059.us-east-1.gamma.docdb-allscale.cascades.docdb.aws.dev:27017/?tls=true&retryWrites=false"

	//EC2 公钥路径
	caFilePath = "/home/ec2-user/rds-combined-ca-bundle.pem"
	DB_NAME    = "test" //project_id
	COL_NAME   = "user"
)

var (
	document = []interface{}{
		User{0, "pig", time.Now()},
		User{1, "dog", time.Now()},
		User{2, "cat", time.Now()},
		User{3, "hen", time.Now()},
		User{4, "pig", time.Now()},
	}
	gCommonClient     *MongoClient
	gProjectClientMap = make(map[string]*MongoClient)
)

type User struct {
	ID        int       `bson:"_id"`       // 用户ID
	Name      string    `bson:"name"`      // 用来推荐的标签数据
	UpdatedAt time.Time `bson:"updatedAt"` // 更新时间
}

func (U *User) equals(I interface{}) bool {
	if I == nil {
		return false
	}
	u := I.(User)
	return U.ID == u.ID && U.Name == u.Name
}

//TestMongo Test All Function
func TestMongo(t *testing.T) {
	t.Run("TestConnect", TestConnect)
	t.Run("TestConnectMultiClient", TestConnectMultiClient)
	t.Run("TestFindOne", TestFindOne)
	t.Run("TestFindAll", TestFindAll)
	t.Run("TestAggregate", TestAggregate)
	t.Run("TestFindOneAndUpdate", TestFindOneAndUpdate)
	t.Run("TestFindOneAndDelete", TestFindOneAndDelete)
	t.Run("TestUpdateOne", TestUpdateOne)
	t.Run("TestUpdateAll", TestUpdateAll)
	t.Run("TestUpsertOne", TestUpsertOne)
	t.Run("TestInsertOne", TestInsertOne)
	t.Run("TestInsertMany", TestInsertMany)
	t.Run("TestDeleteOne", TestDeleteOne)
	t.Run("TestDeleteMany", TestDeleteMany)
	t.Run("TestCount", TestCount)
	t.Run("TestBulkWrite", TestBulkWrite)
	t.Run("TestNewMyCursorAndAll", TestNewMyCursorAndAll)
}

func TestConnect(t *testing.T) {
	defer func() {
		t.Log("==================TestConnect end====================")
	}()
	t.Log("==================TestConnect begin==================")

	c, err := NewMongoClient(URI)
	if err != nil {
		t.Fatal(err)
	}

	db := c.Database(DB_NAME)
	if db == nil {
		t.Fatal("get zmgo err")
	}

	col := c.DbColl(DB_NAME, COL_NAME)
	if col == nil {
		t.Fatal("get collection err")
	}
}

func TestConnectMultiClient(t *testing.T) {
	defer func() {
		t.Log("==================TestConnectMultiClient end====================")
	}()
	t.Log("==================TestConnectMultiClient begin==================")

	var addrs = make(map[string]string)
	//可以从这里添加你的 客户端key 和 客户端uri 的键值对
	addrs[DB_NAME] = URI

	for key, uri := range addrs {
		client, err := NewMongoClient(uri)
		if err != nil {
			t.Fatal(err)
		}

		if key == "default" {
			gCommonClient = client
		} else {
			gProjectClientMap[key] = client
		}
	}

	setFindClient(t)

	c, err := findClient(DB_NAME)
	if err != nil || c == nil {
		t.Fatalf("get client err:%v", err)
	}

	col := Collection(DB_NAME, COL_NAME)
	if col == nil {
		t.Fatal("get collection error")
	}
}

func TestFindOne(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestFindOne end====================")
	}()
	t.Log("==================TestFindOne begin==================")

	initMongoClient(t)
	insertTestData(t)

	_id := 3
	var result *User
	err := FindOne(&result, DB_NAME, COL_NAME, bson.M{"_id": _id})
	if err != nil {
		t.Fatal(err)
	}

	if !result.equals(document[_id]) {
		t.Fatal("FindOne Fail")
	}
	t.Log(fmt.Sprintf("FindOne Success: %+v", result))
}

func TestFindAll(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestFindAll end====================")
	}()
	t.Log("==================TestFindAll begin==================")

	initMongoClient(t)
	insertTestData(t)

	result := make([]*User, 0)
	err := FindAll(&result, DB_NAME, COL_NAME, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	for _id, v := range result {
		t.Log(fmt.Sprintf("%+v", v))
		if !v.equals(document[_id]) {
			t.Fatal("FindAll Fail")
		}
	}
	t.Log("FindAll Success")
}

func TestAggregate(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestAggregate end====================")
	}()
	t.Log("==================TestAggregate begin==================")

	initMongoClient(t)
	insertTestData(t)
	skipNum, gteNum := 1, 1
	result := make([]*User, 0)
	resultDocument := document[gteNum+skipNum:]
	sort.Slice(resultDocument, func(i, j int) bool {
		return resultDocument[i].(User).Name < resultDocument[j].(User).Name
	})
	pipeline := []bson.M{
		{"$match": bson.M{"_id": bson.M{"$gte": gteNum}}},
		{"$skip": skipNum},
		{"$sort": bson.M{"name": 1}},
	}
	err := Aggregate(&result, DB_NAME, COL_NAME, pipeline)
	if err != nil {
		t.Fatal(err)
	}

	for _id, v := range result {
		t.Log(fmt.Sprintf("%+v", v))
		if !v.equals(resultDocument[_id]) {
			t.Fatal("Aggregate Fail")
		}
	}
	t.Log("Aggregate Success")
}

func TestFindOneAndUpdate(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestFindOneAndUpdate end====================")
	}()
	t.Log("==================TestFindOneAndUpdate begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter_pig := bson.M{"name": "pig"}
	filter_bigPig := bson.M{"name": "bigPig"}
	oldCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	oldCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document before FindOneAndUpdate : %d", oldCount_pig)
	t.Logf("the count of bigPig document before FindOneAndUpdate : %d", oldCount_bigPig)

	t.Log("[FindOneAndUpdate document's name to bigPig]")
	update := bson.M{"$set": filter_bigPig}
	err = FindOneAndUpdate(DB_NAME, COL_NAME, filter_pig, update)
	if err != nil {
		t.Fatal(err)
	}

	newCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	newCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document after FindOneAndUpdate : %d", newCount_pig)
	t.Logf("the count of bigPig document after FindOneAndUpdate : %d", newCount_bigPig)

	if newCount_pig != oldCount_pig-1 && newCount_bigPig != oldCount_bigPig+1 {
		t.Fatal("FindOneAndUpdate Fail")
	}
	t.Log("FindOneAndUpdate Success")
}

func TestFindOneAndDelete(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestFindOneAndDelete end====================")
	}()
	t.Log("==================TestFindOneAndDelete begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter := bson.M{"_id": bson.M{"$gte": 2}}
	oldCount, err := Count(DB_NAME, COL_NAME, filter)
	t.Logf("the count of document before FindOneAndDelete : %d", oldCount)

	err = FindOneAndDelete(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}

	newCount, err := Count(DB_NAME, COL_NAME, filter)
	t.Logf("the count of document after FindOneAndDelete : %d", newCount)
	if newCount != oldCount-1 {
		t.Fatal("FindOneAndDelete Fail")
	}
	t.Log(fmt.Sprintf("FindOneAndDelete Success"))
}

func TestUpdateOne(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestUpdateOne end====================")
	}()
	t.Log("==================TestUpdateOne begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter_pig := bson.M{"name": "pig"}
	filter_bigPig := bson.M{"name": "bigPig"}
	oldCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	oldCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document before UpdateOne : %d", oldCount_pig)
	t.Logf("the count of bigPig document before UpdateOne : %d", oldCount_bigPig)

	t.Log("[updateOne document's name to bigPig]")
	update := bson.M{"$set": filter_bigPig}
	err = UpdateOne(DB_NAME, COL_NAME, filter_pig, update)
	if err != nil {
		t.Fatal(err)
	}

	newCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	newCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document after UpdateOne : %d", newCount_pig)
	t.Logf("the count of bigPig document after UpdateOne : %d", newCount_bigPig)

	if newCount_pig != oldCount_pig-1 && newCount_bigPig != oldCount_bigPig+1 {
		t.Fatal("TestUpdateOne Fail")
	}
	t.Log(fmt.Sprintf("TestUpdateOne Success"))
}

func TestUpdateAll(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestUpdateAll end====================")
	}()
	t.Log("==================TestUpdateAll begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter_pig := bson.M{"name": "pig"}
	filter_bigPig := bson.M{"name": "bigPig"}
	oldCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	oldCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document before UpdateAll : %d", oldCount_pig)
	t.Logf("the count of bigPig document before UpdateAll : %d", oldCount_bigPig)

	t.Log("[UpdateAll document's name to bigPig]")
	update := bson.M{"$set": filter_bigPig}
	err = UpdateAll(DB_NAME, COL_NAME, filter_pig, update)
	if err != nil {
		t.Fatal(err)
	}

	newCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	newCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document after UpdateAll : %d", newCount_pig)
	t.Logf("the count of bigPig document after UpdateAll : %d", newCount_bigPig)

	if newCount_pig != 0 && newCount_bigPig != oldCount_pig+oldCount_bigPig {
		t.Fatal("TestUpdateAll Fail")
	}
	t.Log(fmt.Sprintf("TestUpdateAll Success"))
}

func TestUpsertOne(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestUpsertOne end====================")
	}()
	t.Log("==================TestUpsertOne begin==================")

	initMongoClient(t)
	filter_pig := bson.M{"name": "pig"}
	filter_bigPig := bson.M{"name": "bigPig"}

	t.Log("==================no document and upsert = insert==================")

	//之前没有pig 所以会插入
	oldCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	oldCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document before UpsertOne : %d", oldCount_pig)
	t.Logf("the count of bigPig document before UpsertOne : %d", oldCount_bigPig)

	t.Log("[UpsertOne document's name from pig to bigPig]")
	update := bson.M{"$set": filter_bigPig}
	err = UpsertOne(DB_NAME, COL_NAME, filter_pig, update)
	if err != nil {
		t.Fatal(err)
	}

	newCount_pig, err := Count(DB_NAME, COL_NAME, filter_pig)
	newCount_bigPig, err := Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document after UpsertOne : %d", newCount_pig)
	t.Logf("the count of bigPig document after UpsertOne : %d", newCount_bigPig)

	if newCount_bigPig != oldCount_bigPig+1 {
		t.Fatal("UpsertOne Fail")
	}
	t.Log("==================have document and upsert = update==================")

	//已经有了bigPig 所以会更新
	oldCount_pig, err = Count(DB_NAME, COL_NAME, filter_pig)
	oldCount_bigPig, err = Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document before UpsertOne : %d", oldCount_pig)
	t.Logf("the count of bigPig document before UpsertOne : %d", oldCount_bigPig)

	t.Log("[UpsertOne document's t name from bigPig to pig]")
	update = bson.M{"$set": filter_pig}
	err = UpdateOne(DB_NAME, COL_NAME, filter_bigPig, update)
	if err != nil {
		t.Fatal(err)
	}

	newCount_pig, err = Count(DB_NAME, COL_NAME, filter_pig)
	newCount_bigPig, err = Count(DB_NAME, COL_NAME, filter_bigPig)
	t.Logf("the count of pig document after UpsertOne : %d", newCount_pig)
	t.Logf("the count of bigPig document after UpsertOne : %d", newCount_bigPig)

	if newCount_pig != oldCount_pig+1 && newCount_bigPig != oldCount_bigPig-1 {
		t.Fatal("UpsertOne Fail")
	}
	t.Log(fmt.Sprintf("UpsertOne Success"))
}

func TestInsertOne(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestInsertOne end====================")
	}()
	t.Log("==================TestInsertOne begin==================")

	initMongoClient(t)

	var result *User
	doc := &User{520, "pig", time.Now()}
	t.Log("[Insert document: {_id:520, name:pig, updateAt:time.Now()}]")

	_, err := InsertOne(DB_NAME, COL_NAME, doc)
	if err != nil {
		t.Fatal(err)
	}

	err = FindOne(&result, DB_NAME, COL_NAME, doc)
	if err != nil || !result.equals(*doc) {
		t.Fatal("TestInsertOne Fail")
	}
	t.Log("TestInsertOne Success")
}

func TestInsertMany(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestInsertMany end====================")
	}()
	t.Log("==================TestInsertMany begin==================")

	initMongoClient(t)
	t.Log("[Insert 5 documents {_id, name, updateAt} and the _id is in [0, 1, 2, 3, 4]]")
	insertTestData(t)

	result := make([]*User, 0)
	filter := bson.M{}
	err := FindAll(&result, DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("After insert and findAll documents result :")
	for _id, v := range result {
		t.Log(fmt.Sprintf("%+v", v))
		if !v.equals(document[_id]) {
			t.Fatal("TestInsertMany Fail")
		}
	}
	t.Log("TestInsertMany Success")
}

func TestDeleteOne(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestDeleteOne end====================")
	}()
	t.Log("==================TestDeleteOne begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter := bson.M{"_id": bson.M{"$gte": 2}}
	oldCount, err := Count(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("the Count of Document id >= 2 before DeleteOne :", oldCount)

	t.Log("[DeleteOne which document id >= 2]")
	err = DeleteOne(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}

	newCount, err := Count(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("the Count of Document id >= 2 after DeleteOne :", newCount)

	if newCount != oldCount-1 {
		t.Fatal("TestDeleteOne Fail")
	}
	t.Log("TestDeleteOne Success")
}

func TestDeleteMany(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestDeleteMany end====================")
	}()
	t.Log("==================TestDeleteMany begin==================")

	initMongoClient(t)
	insertTestData(t)

	filter := bson.M{"_id": bson.M{"$gte": 2}}
	oldCount, err := Count(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("the Count of Document id >= 2 before deleteMany :", oldCount)

	t.Log("[deleteMany which document id >= 2]")
	err = DeleteMany(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}

	newCount, err := Count(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("the Count of Document id >= 2 after deleteMany :", newCount)

	if newCount != 0 {
		t.Fatal("TestDeleteMany Fail")
	}
	t.Log("TestDeleteMany Success")
}

func TestCount(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestCount end====================")
	}()
	t.Log("==================TestCount begin==================")

	initMongoClient(t)
	t.Log("[insert 5 document]")
	insertTestData(t)

	filter := bson.M{}
	count, err := Count(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatal("TestCount Fail")
	}
	t.Log("TestCount Success")
}

func TestBulkWrite(t *testing.T) {
	defer func() {
		deleteTestData(t)
		t.Log("==================TestBulkWrite end====================")
	}()
	t.Log("==================TestBulkWrite begin==================")

	initMongoClient(t)

	t.Log("[writeModel is upsert 5 document]")
	operations := make([]mongo.WriteModel, 0)
	for i := 0; i < 5; i++ {
		query := bson.M{"_id": document[i].(User).ID}
		update := bson.M{"$set": bson.M{"name": document[i].(User).Name}}
		operation := mongo.NewUpdateOneModel().SetFilter(query).SetUpdate(update).SetUpsert(true)
		operations = append(operations, operation)
	}

	err := BulkWrite(DB_NAME, COL_NAME, operations)
	if err != nil {
		t.Fatal(err)
	}

	result := make([]*User, 0)
	err = FindAll(&result, DB_NAME, COL_NAME, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Document of after BulkWrite:")
	for i, v := range result {
		t.Log(fmt.Sprintf("%+v", v))
		if !v.equals(document[i]) {
			t.Fatal("BulkWrite Fail")
		}
	}
	t.Log("BulkWrite Success")
}

func TestNewMyCursorAndAll(t *testing.T) {
	t.Log("==================TestNewMyCursorAndAll begin==================")
	c, err := NewMongoClient(URI)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = c.DeleteMany(DB_NAME, COL_NAME, bson.M{})
		if err != nil {
			t.Fatal(err)
		}
		t.Log("[delete test data success]")
		t.Log("==================TestNewMyCursorAndAll end====================")
	}()

	result := make([]*User, 0)
	ctx := context.Background()

	t.Log("[insert two document for test]")
	_, err = c.InsertMany(DB_NAME, COL_NAME, document)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := c.DbColl(DB_NAME, COL_NAME).Find(ctx, bson.M{})
	if err != nil {
		t.Fatal(err)
	}

	err = NewMyCursor(cursor).All(&result)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("NewMyCursor().All(&result) decode result:")
	for _id, v := range result {
		if !v.equals(document[_id]) {
			t.Fatal("TestNewMyCursorAndAll Fail")
		}
		t.Log(v)
	}
	t.Log("TestNewMyCursorAndAll Success")
}

/**
 * Function for TestUnit
 */

//setFindClient 传递FindClient方法
func setFindClient(t *testing.T) {
	SetFindClient(
		func(projectId string) (*MongoClient, error) {
			var client *MongoClient
			if c, ok := gProjectClientMap[projectId]; ok {
				client = c
			} else {
				client = gCommonClient
			}
			return client, nil
		})
	t.Log("SetSendClient Success")
}

//initMongoClient 初始化mongo客户端并传递FindClient方法
func initMongoClient(t *testing.T) {
	var addrs = make(map[string]string)
	addrs[DB_NAME] = URI

	for key, uri := range addrs {
		//client, err := NewDocumentClient(uri,caFilePath)
		client, err := NewMongoClient(uri)
		if err != nil {
			t.Fatalf("initMongoClient fail=>%+v", err)
		}

		if key == "default" {
			gCommonClient = client
		} else {
			gProjectClientMap[key] = client
		}
	}
	setFindClient(t)
}

//insertTestData 插入测试数据
func insertTestData(t *testing.T) {
	deleteTestData(t)
	_, err := InsertMany(DB_NAME, COL_NAME, document)
	if err != nil {
		t.Fatalf("insertTestData fail=> %+v", err)
	}
	t.Log("insertTestData Success")
}

//deleteTestData 清空测试数据
func deleteTestData(t *testing.T) {
	filter := bson.D{}
	err := DeleteMany(DB_NAME, COL_NAME, filter)
	if err != nil {
		t.Fatalf("deleteTestData fail=> %+v", err)
	}
	t.Log("deleteTestData Success")
}
