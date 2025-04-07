package main

import (
	"context"
	"fmt"
	"kvstorage/gen-go/kv"
	"net/http"

	_ "kvstorage/docs"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

type ThriftClient struct {
	client *kv.StorageServiceClient
}

func NewThriftClient() (*ThriftClient, error) {
	transport, err := thrift.NewTSocket("localhost:9090")
	if err != nil {
		return nil, err
	}

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	useTransport, err := transportFactory.GetTransport(transport)
	if err != nil {
		return nil, err
	}

	client := kv.NewStorageServiceClientFactory(useTransport, protocolFactory)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	return &ThriftClient{client: client}, nil
}

var thriftClient, err = NewThriftClient()

// @Summary Get data by key
// @Description Get data from storage by key
// @Tags data
// @Accept json
// @Produce json
// @Param key path string true "Key"
// @Success 200 {object} kv.DataResult_
// @Failure 500 {object} []kv.ErrorCode
// @Router /api/getdata/{key} [get]
func getData(c *gin.Context) {
	key := c.Param("key")
	// Get data
	dataResult, err := thriftClient.client.GetData(context.Background(), key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Error getting data: %v", err)})
		return
	}
	fmt.Printf("Got data: %v, error code: %v\n", dataResult.Data, dataResult.ErrorCode)
	c.JSON(http.StatusOK, dataResult)
}

// @Summary Put data
// @Description Put data into storage
// @Tags data
// @Accept json
// @Produce json
// @Param item body kv.MapItem true "Map Item"
// @Success 200 {object} []kv.ErrorCode
// @Failure 400 {object} []kv.ErrorCode
// @Failure 500 {object} []kv.ErrorCode
// @Router /api/putdata [post]
func putData(c *gin.Context) {
	// Put data
	var mapItem kv.MapItem
	if err := c.ShouldBindJSON(&mapItem); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	errorCode, err := thriftClient.client.PutData(context.Background(), mapItem.Key, &mapItem)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ResponseCode": errorCode})
}

// @Summary Get list of data by keys
// @Description Get list of data from storage by keys
// @Tags data
// @Accept json
// @Produce json
// @Param keys body []string true "List of Keys"
// @Success 200 {object} []kv.MapItem
// @Failure 400 {object} []kv.ErrorCode
// @Failure 500 {object} []kv.ErrorCode
// @Router /api/getlistdata [get]
func getListData(c *gin.Context) {
	var keys []string
	if err := c.ShouldBindJSON(&keys); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	listData, err := thriftClient.client.GetListData(context.Background(), keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, listData)
}

// @Summary Remove data by key
// @Description Remove data from storage by key
// @Tags data
// @Accept json
// @Produce json
// @Param key path string true "Key"
// @Success 200 {object} []kv.ErrorCode
// @Failure 500 {object} []kv.ErrorCode
// @Router /api/removedata/{key} [delete]
func removeData(c *gin.Context) {
	key := c.Param("key")
	errorCode, err := thriftClient.client.RemoveData(context.Background(), key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"errorCode": errorCode})
}

// @Summary Put multiple data
// @Description Put multiple data into storage
// @Tags data
// @Accept json
// @Produce json
// @Param items body []kv.MapItem true "List of Map Items"
// @Success 200 {object} []kv.ErrorCode
// @Failure 400 {object} []kv.ErrorCode
// @Failure 500 {object} []kv.ErrorCode
// @Router /api/putmultidata [post]
func putMultiData(c *gin.Context) {
	var items []*kv.MapItem
	if err := c.ShouldBindJSON(&items); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	errorCode, err := thriftClient.client.PutMultiData(context.Background(), items)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"errorCode": errorCode})
}

// @title Data Service API
// @version 1.0
// @description A Data service API in Go using Gin framework

// @host localhost:8080
// @BasePath /api
func main() {
	// transport, err := thrift.NewTSocket("localhost:9090")
	// if err != nil {
	// 	log.Fatalf("Error opening socket: %v", err)
	// }

	// transportFactory := thrift.NewTBufferedTransportFactory(8192)
	// protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	// useTransport, err := transportFactory.GetTransport(transport)
	// if err != nil {
	// 	log.Fatalf("Error getting transport: %v", err)
	// }

	// client := kv.NewStorageServiceClientFactory(useTransport, protocolFactory) // Sử dụng StorageServiceClientFactory

	// if err := transport.Open(); err != nil {
	// 	log.Fatalf("Error opening transport: %v", err)
	// }
	// defer transport.Close()

	router := gin.Default()
	//add swagger
	router.GET("docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Define routers
	router.GET("/api/getdata/:key", getData)
	router.POST("/api/putdata", putData)
	router.GET("/api/getlistdata", getListData)
	router.DELETE("/api/removedata/:key", removeData)
	router.POST("/api/putmultidata", putMultiData)

	router.Run("localhost:8080")

}
