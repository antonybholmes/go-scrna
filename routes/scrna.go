package routes

import (
	"errors"
	"strconv"

	scrnadbcache "github.com/antonybholmes/go-scrna/scrnadb"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth"
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

func getUser(c *gin.Context) (*auth.AuthUserJwtClaims, error) {

	var user *auth.AuthUserJwtClaims

	if v, exists := c.Get("user"); exists {

		user = v.(*auth.AuthUserJwtClaims)
	} else {
		return nil, errors.New("no user in context")
	}

	return user, nil
}

func ScrnaDatasetsRoute(c *gin.Context) {

	species := c.Param("species")
	assembly := c.Param("assembly")

	user, err := getUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	log.Debug().Msgf("scrna user %v", user)

	datasets, err := scrnadbcache.Datasets(species, assembly, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", datasets)
}

// Gets expression data from a given dataset
func ScrnaGexRoute(c *gin.Context) {
	datasetId := c.Param("id")

	if datasetId == "" {
		c.Error(errors.New("missing id"))
		return
	}

	params, err := parseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	user, err := getUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	err = scrnadbcache.HasPermissionToViewDataset(datasetId, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	// default to rna-seq
	ret, err := scrnadbcache.Gex(datasetId, params.Genes)

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
	datasetId := c.Param("id")

	if datasetId == "" {
		c.Error(errors.New("id missing"))
		return
	}

	q := c.Query("q")

	if q == "" {
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

	safeQuery := query.SanitizeQuery(q)

	log.Debug().Msgf("safe %s", safeQuery)

	user, err := getUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	err = scrnadbcache.HasPermissionToViewDataset(datasetId, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	ret, err := scrnadbcache.SearchGenes(datasetId, safeQuery, limit)

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
