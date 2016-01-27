package zscore

import (
	"github.com/tucobenedicto/mongoDBConfig"
)

// A visitor is an indivdual, unique customer
// Visitor struct follows structure of db so we can unmarshal
// a unique Id and a collection of summaries
type Visitor struct {
	Id        mongoDBConfig.VisitorId `bson:"_id,omitempty" json:"id"`
	Summaries Summaries               `bson:"summaries,omitempty" json:"summaries"`
}

// there's more data in summaries, but we're interested in these two
type Summaries struct {
	Amount               float64 `bson:"amt" json:"amt"`
	NumberOfTransactions uint32  `bson:"trn" json:"trn"`
}

// bson and json so we update db using mgo
type ZscoreData struct {
	Amt float64 `bson:"amt" json:"amt"`
	Trn float64 `bson:"trn" json:"trn"`
}
