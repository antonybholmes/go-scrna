package routes

import (
	"errors"
	"fmt"
	"strconv"

	scrnadbcache "github.com/antonybholmes/go-scrna/cache"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/gin-gonic/gin"
)

const DefaultLimit int16 = 20

type ScrnaParams struct {
	Genes []string `json:"genes"`
}

func parseParamsFromPost(c *gin.Context) (*ScrnaParams, error) {

	var params ScrnaParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	return &params, nil
}

func ScrnaSpeciesRoute(c *gin.Context) {

	types, err := scrnadbcache.Species()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", types)
}

func ScrnaAssembliesRoute(c *gin.Context) {

	species := c.Param("species")

	// technologies, err := gexdbcache.AllTechnologies() //gexdbcache.Technologies() //species)

	// if err != nil {
	// 	c.Error(err)
	// 	return
	// }

	assemblies, err := scrnadbcache.Assemblies(species) //gexdbcache.Technologies()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", assemblies)
}

// func GexValueTypesRoute(c *gin.Context) {

// 	params, err := parseParamsFromPost(c)

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	valueTypes, err := gexdbcache.GexValueTypes(params.Platform.Id)

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	web.MakeDataResp(c, "", valueTypes)
// }

func ScrnaDatasetsRoute(c *gin.Context) {

	species := c.Param("species")
	assembly := c.Param("assembly")

	datasets, err := scrnadbcache.Datasets(species, assembly)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", datasets)
}

// Gets expression data from a given dataset
func ScrnaGexRoute(c *gin.Context) {
	id := c.Param("id")

	if id == "" {
		c.Error(fmt.Errorf("missing id"))
		return
	}

	params, err := parseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	// default to rna-seq
	ret, err := scrnadbcache.Gex(id, params.Genes)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", ret)
}

// func ScrnaMetadataRoute(c *gin.Context) {
// 	publicId := c.Param("id")

// 	if publicId == "" {
// 		c.Error(fmt.Errorf("missing id"))
// 		return
// 	}

// 	ret, err := scrnadbcache.Metadata(publicId)

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	web.MakeDataResp(c, "", ret)
// }

func ScrnaMetadataRoute(c *gin.Context) {
	id := c.Param("id")

	if id == "" {
		c.Error(errors.New("missing id"))
		return
	}

	ret, err := scrnadbcache.Metadata(id)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", ret)
}

func ScrnaGenesRoute(c *gin.Context) {
	publicId := c.Param("id")

	if publicId == "" {
		c.Error(errors.New("missing id"))
		return
	}

	ret, err := scrnadbcache.Genes(publicId)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", ret)
}

func ScrnaSearchGenesRoute(c *gin.Context) {
	publicId := c.Param("id")

	if publicId == "" {
		c.Error(errors.New("id missing"))
		return
	}

	query := c.Query("q")

	if query == "" {
		c.Error(errors.New("query missing"))
		return
	}

	limit := DefaultLimit

	if c.Query("limit") != "" {
		v, err := strconv.Atoi(c.Query("limit"))

		if err == nil {
			limit = int16(v)
		}
	}

	safeQuery := sys.SanitizeQuery(query)

	log.Debug().Msgf("safe %s", safeQuery)

	ret, err := scrnadbcache.SearchGenes(publicId, safeQuery, limit)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", ret)
}

// func GexRoute(c *gin.Context) {
// 	gexType := c.Param("type")

// 	params, err := ParseParamsFromPost(c)

// 	if err != nil {
// 		return web.ErrorReq(err)
// 	}

// 	search, err := gexdbcache.GetInstance().Search(gexType, params.Datasets, params.Genes)

// 	if err != nil {
// 		return web.ErrorReq(err)
// 	}

// 	web.MakeDataResp(c, "", search)

// 	//web.MakeDataResp(c, "", mutationdbcache.GetInstance().List())
// }
