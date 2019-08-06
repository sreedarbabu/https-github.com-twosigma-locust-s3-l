/*
Copyright 2019 TWO SIGMA OPEN SOURCE, LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package v2

import (
	"github.com/minio/minio-go/pkg/s3signer"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
)

// S3v2signer is a named request handler the SDK will use to sign
// service client request with using the V2 signature.
var S3v2signer = request.NamedHandler{
	Name: "v2.SignRequestHandler", Fn: SignSDKRequest,
}

// SignSDKRequest requests with signature version 2.
//
// Will sign the requests with the service config's Credentials object
// Signing is skipped if the credentials is the credentials.AnonymousCredentials
// object.
func SignSDKRequest(req *request.Request) {
	// If the request does not need to be signed ignore the signing of the
	// request if the AnonymousCredentials object is used.
	if req.Config.Credentials == credentials.AnonymousCredentials {
		return
	}

	credValue, err := req.Config.Credentials.Get()
	if err != nil {
		return
	}
	// hard coded not to support virtual host style. this need to be fixed in future
	s3signer.SignV2(*req.HTTPRequest, credValue.AccessKeyID, credValue.SecretAccessKey, false)
}
