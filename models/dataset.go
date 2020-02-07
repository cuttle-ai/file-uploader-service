// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package models

import (
	"github.com/jinzhu/gorm"
)

const (
	//DatasetSourceFile indicates that the dataset source is file
	DatasetSourceFile = "FILE"
)

//Dataset represents a dataset
type Dataset struct {
	gorm.Model
	//Name of the dataset
	Name string
	//Description is the description for the dataset
	Description string
	//UserID is the id of the user with whom the file is associated with
	UserID uint
	//Source is the type of dataset source. It can be file, database etc
	Source string
	//ResourceID is the lid of the underlying dataset like file id for a dataset who source is file
	ResourceID uint
	//UploadedDataset is the uploaded data set info
	UploadedDataset interface{} `gorm:"-"`
}
