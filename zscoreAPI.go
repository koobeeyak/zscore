package zscore

import (
	"log"
	"math"

	"github.com/tucobenedicto/mongoDBConfig"
	"gopkg.in/mgo.v2/bson"
)

// apply logarithmic transformation to amounts and number of transactions to normalize
// will more closely resemble a normal distribution so we can take z-scores
// aggregate a sum, then divide by total count to find means
// return as a struct of means
func getMeans(id mongoDBConfig.BrandId) (mean ZscoreData) {
	collections := mongoDBConfig.NewDBConn()
	visitorsCollection := collections.C("visitors")
	result := visitorsCollection.
		Find(bson.M{
		"brandid":       id,               // for which brand in our db are we computing z-scores
		"summaries.amt": bson.M{"$gt": 0}, // don't include $0 transactions
	}).
		Select(bson.M{
		"summaries.amt": 1,
		"summaries.trn": 1}).
		//Limit(100).
		Iter()

	visitor := Visitor{}
	count := 0
	// mgo next() will iterate through the dataset and unmarshal into vistor struct
	for result.Next(&visitor) {
		count += 1

		amt := visitor.Summaries.Amount
		amt = math.Log10(amt)
		mean.Amt += amt

		trn := float64(visitor.Summaries.NumberOfTransactions)
		trn = math.Log10(trn)
		mean.Trn += trn
	}

	mean.Amt = mean.Amt / float64(count)
	mean.Trn = mean.Trn / float64(count)
	return
}

// calculate standard deviation, also applying logarithmic transformation
func getStdDevs(id mongoDBConfig.BrandId, mean ZscoreData) (stdDev ZscoreData) {
	collections := mongoDBConfig.NewDBConn()
	visitorsCollection := collections.C("visitors")
	result := visitorsCollection.
		Find(bson.M{
		"brandid":       id,
		"summaries.amt": bson.M{"$gt": 0},
	}).
		Select(bson.M{
		"summaries.amt": 1,
		"summaries.trn": 1}).
		Iter()

	visitor := Visitor{}
	// let's get variances first, then standard deviations
	variance := ZscoreData{}
	count := 0

	for result.Next(&visitor) {
		count += 1

		amt := visitor.Summaries.Amount
		amt = math.Log10(amt)
		amt = mean.Amt - amt
		variance.Amt += (amt * amt)

		trn := float64(visitor.Summaries.NumberOfTransactions)
		trn = math.Log10(trn)
		trn = mean.Trn - trn
		variance.Trn += (trn * trn)
	}
	// divide by total count - 1 to find variance
	variance.Amt = variance.Amt / float64(count-1)
	variance.Trn = variance.Trn / float64(count-1)

	stdDev.Amt = math.Sqrt(variance.Amt)
	stdDev.Trn = math.Sqrt(variance.Trn)
	return
}

// subtract from mean, divide by standard deviation to get z-score
// update db with new z-scores
func updateVisitorsWithZScore(id mongoDBConfig.BrandId, mean ZscoreData, stdDev ZscoreData) bool {
	collections := mongoDBConfig.NewDBConn()
	visitorsCollection := collections.C("visitors")
	visitorsUpdater := collections.C("visitors")

	result := visitorsCollection.
		Find(bson.M{
		"brandid":       id,
		"summaries.amt": bson.M{"$gt": 0},
	}).
		Select(bson.M{
		"_id":           1,
		"summaries.amt": 1,
		"summaries.trn": 1}).
		Iter()

	v := Visitor{}
	zscore := ZscoreData{}
	for result.Next(&v) {
		amt := v.Summaries.Amount
		amt = math.Log10(amt)
		zscore.Amt = (amt - mean.Amt) / stdDev.Amt

		trn := float64(v.Summaries.NumberOfTransactions)
		trn = math.Log10(trn)
		zscore.Trn = (trn - mean.Trn) / stdDev.Trn

		set := bson.M{
			"zscore": zscore,
		}
		// mgo UpdateID() will update entry whose _id matches id argument
		if err := visitorsUpdater.UpdateId(v.Id, bson.M{"$set": set}); err != nil {
			log.Printf("Got an error while updating visitor: %v\n", err)
			return false
		}
	}

	return true
}

// public function to run all calculations and update db
func RunUpdate(id mongoDBConfig.BrandId) bool {
	mean := getMeans(id)
	stdDev := getStdDevs(id, mean)
	result := updateVisitorsWithZScore(id, mean, stdDev)
	return result
}
