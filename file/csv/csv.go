// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package csv has the implementation of the file interface for csv files
package csv

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Clever/csvlint"
	brainModels "github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/db-toolkit/datastores/services"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/octopus/interpreter"
	"github.com/google/uuid"
)

//CSV handles the fiels of csv type
type CSV struct {
	//Filename is the name of the file
	Filename string
	//Name is the dataset name
	Name string
	//Resource holds the db instance of the underlying file
	Resource db.FileUpload
	//Table is the underlying octopus table node
	Table *interpreter.TableNode
}

//ID returns the underlying file's id in db
func (c CSV) ID() uint {
	return c.Resource.ID
}

//Store stores the csv info to database
func (c *CSV) Store(a *config.AppContext) (*brainModels.Dataset, error) {
	/*
	 * We will db transaction we have to save the file upload and the dataset info
	 * Then we will create the file upload
	 * Then we will create the dataset with resource id as the of the file
	 * Then we will create the dataset user mappings
	 */
	//starting the transaction
	tx := a.Db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		//error while beginning the transaction
		return nil, err
	}

	//saving the file upload
	fileRecord := &models.FileUpload{Name: c.Name, UserID: a.Session.User.ID, Location: c.Filename, Status: models.FileUploadStatusUploaded, Type: models.FileUploadTypeCSV}
	if err := tx.Create(fileRecord).Error; err != nil {
		//error while creating the upload
		tx.Rollback()
		a.Log.Error("error while creating the file upload record")
		return nil, err
	}

	//saving the dataset record
	dataset := &brainModels.Dataset{Name: c.Name, UserID: fileRecord.UserID, ResourceID: fileRecord.ID, Source: brainModels.DatasetSourceFile}
	if err := tx.Create(dataset).Error; err != nil {
		//error while creating the dataset
		tx.Rollback()
		a.Log.Error("error while creating the datset record")
		return nil, err
	}
	dataset.UploadedDataset = fileRecord

	//creating the dataset user mappings
	datasetMapping := &brainModels.DatsetUserMapping{DatasetID: dataset.ID, UserID: fileRecord.UserID, AccessType: brainModels.DatasetAccessTypeCreator}
	if err := tx.Create(datasetMapping).Error; err != nil {
		//error while creating the dataset user mapping
		tx.Rollback()
		a.Log.Error("error while creating the datset user mapping")
		return nil, err
	}

	//returning the result
	return dataset, tx.Commit().Error
}

//Validate will validate the csv file and returns the errors existing while parsing the csv file
func (c *CSV) Validate() ([]error, error) {
	/*
	 * We will open the file
	 * Then we will validate the same
	 * return the errors if any
	 */
	f, err := os.Open(c.Filename)
	if err != nil {
		c.Resource.Status = models.FileUploadStatusValidatingError
		return nil, err
	}
	defer f.Close()
	invalids, _, err := csvlint.Validate(f, rune(','), true)
	if err != nil {
		c.Resource.Status = models.FileUploadStatusValidatingError
		return nil, err
	}
	c.Resource.Status = models.FileUploadStatusValidated
	if len(invalids) == 0 {
		return nil, nil
	}
	errorResults := []error{}
	for _, v := range invalids {
		errorResults = append(errorResults, v)
	}
	return errorResults, nil
}

//IdentifyColumns will identify the columns in the file and store them in the database
func (c *CSV) IdentifyColumns(columns []interpreter.ColumnNode) ([]interpreter.ColumnNode, error) {
	/*
	 * We will open the file
	 * Will read the column names
	 * We will read the csv file line by line
	 * Then we will try to predict the columns
	 */

	//opening the file
	f, err := os.Open(c.Filename)
	if err != nil {
		//error while opening the file
		return nil, err
	}

	//reading the column names in the file
	r := csv.NewReader(f)
	cols, err := r.Read()
	//even if the error was EOF or aything else, we will report it as error since
	//we couldn't read the cols
	if err != nil && err != io.EOF {
		return nil, err
	}
	if err != nil && err == io.EOF {
		return nil, errors.New("EOF reached before able to read the columns in the file")
	}
	//storing the columns in the columns result

	if len(columns) == 0 {
		columns = []interpreter.ColumnNode{}
		for k, col := range cols {
			columns = append(columns, interpreter.ColumnNode{
				UID:  uuid.New().String(),
				Name: strconv.Itoa(k),
				//Will keep the default data type as string
				DataType: interpreter.DataTypeString,
				Word:     []rune(col),
			})
		}
	}

	//predicting the columns
	//this is a classical bruteforce approach of going through the entire dataset
	//and making sure the data type is accurate
	//have to improve the below piece of code
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for i, v := range record {
			columns[i].DataType, columns[i].DateFormat = predictColumn(v, columns[i].DataType)
			if columns[i].DataType == interpreter.DataTypeInt || columns[i].DataType == interpreter.DataTypeFloat {
				columns[i].AggregationFn = interpreter.AggregationFnSum
			} else {
				columns[i].AggregationFn = interpreter.AggregationFnCount
			}
		}
	}
	return columns, nil
}

func predictColumn(value string, existingType string) (string, string) {
	/*
	 * If the value is empty we will return the existing data type itself
	 * We chek the the value is of same data type for all data types except string type
	 * If the existing data type is string, we will have check whether it can be
	 * float
	 * integer
	 * date
	 */
	//returning existing type if the value is empty
	if len(value) == 0 {
		return existingType, ""
	}

	//checking for date
	if existingType == interpreter.DataTypeDate {
		ft, ok := checkForDates(value)
		if ok {
			return interpreter.DataTypeDate, ft
		}
		return interpreter.DataTypeString, ""
	}

	//checking for float
	if existingType == interpreter.DataTypeFloat {
		tV := strings.TrimSpace(value)
		_, err := strconv.ParseFloat(tV, 64)
		if err == nil {
			return interpreter.DataTypeFloat, ""
		}
		return interpreter.DataTypeString, ""
	}

	//checking for integer
	if existingType == interpreter.DataTypeInt {
		tV := strings.TrimSpace(value)
		sV := strings.TrimSuffix(tV, ".")
		_, err := strconv.ParseInt(sV, 10, 64)
		if err == nil {
			return interpreter.DataTypeInt, ""
		}
		_, errF := strconv.ParseFloat(tV, 64)
		if errF == nil {
			return interpreter.DataTypeFloat, ""
		}
		return interpreter.DataTypeString, ""
	}

	//check for string
	//we check every data type
	ft, ok := checkForDates(value)
	if ok {
		return interpreter.DataTypeDate, ft
	}
	tV := strings.TrimSpace(value)
	_, errF := strconv.ParseFloat(tV, 64)
	if errF == nil {
		return interpreter.DataTypeFloat, ""
	}
	sV := strings.TrimSuffix(tV, ".")
	_, errI := strconv.ParseInt(sV, 10, 64)
	if errI == nil {
		return interpreter.DataTypeInt, ""
	}
	return interpreter.DataTypeString, ""
}

var supportedDateTypes = []string{
	"2006-Jan-02",
	"01/02/2006",
	"1/02/2006",
	"1/2/2006",
}

func checkForDates(value string) (string, bool) {
	for _, v := range supportedDateTypes {
		_, err := time.Parse(v, value)
		if err == nil {
			return v, true
		}
	}
	return "", false
}

//Upload will attempt to upload the file to the analytics engine and report any error occurred
func (c *CSV) Upload(a *config.AppContext, table interpreter.TableNode, appendData bool, createTable bool, dataStore services.Service) error {
	/*
	 * We will first get the underlyign datastore
	 * Then we will upload the data
	 */
	//getting the underlying datastore
	dS, err := dataStore.Datastore()
	if err != nil {
		//error while getting the datastore connection
		a.Log.Error("error while getting the datastore connection")
		return err
	}

	//we start uploading the data
	err = dS.DumpCSV(c.Filename, table.Name, table.Children, appendData, createTable, a.Log)
	if err != nil {
		//error while dumping the csv to the datastore
		a.Log.Error("error while dumping the csv to the datastore")
		return err
	}
	return nil
}

//UpdateStatus updates the status of the file upload in db
func (c *CSV) UpdateStatus(a *config.AppContext) error {
	/*
	 * We will update the status
	 */
	return a.Db.Model(&c.Resource).Updates(map[string]interface{}{
		"status": c.Resource.Status,
	}).Error
}
