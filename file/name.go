// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package file

import (
	"strconv"
	"time"

	"github.com/cuttle-ai/file-uploader-service/config"
)

/*
 * This file contains the utilities required for generating name of the file
 * While working in a concurrent environment name generation has to pipelined
 */

//NameGenerate is the struct to be passed to the GenerateName goroutine
type NameGenerate struct {
	Name      string            //Name is the name to be generated
	Generated string            //Generated file name with athe given name
	Out       chan NameGenerate //Out is the output channel to which the generated name has to be passed
}

//NewNameGenerate intializes the NameGenerate struct with out channel
func NewNameGenerate() NameGenerate {
	return NameGenerate{Out: make(chan NameGenerate)}
}

//GenerateNameChan is the channel used for the GenerateName goroutine intialization
var GenerateNameChan = make(chan NameGenerate)

//GenerateName generates the name of a given file.
func GenerateName(in chan NameGenerate) {
	/*
	 * We will initialize the index
	 * Then we will go into an infinte loop waiting for requests to come in
	 * When a request come in we will generate a name using the time.now and the index
	 * Then will flush it out to the output channel
	 */
	index := int64(0)
	for {
		req := <-in
		req.Generated = req.Name + config.Separator + time.Now().String() + "_" + strconv.FormatInt(index, 10)
		index++
		req.Out <- req
	}
}

func init() {
	/*
	 * Will start the name generation go routine
	 */
	go GenerateName(GenerateNameChan)
}
