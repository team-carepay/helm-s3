package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/pkg/errors"

	"github.com/hypnoglow/helm-s3/internal/awss3"
	"github.com/hypnoglow/helm-s3/internal/helmutil"
)

type initAction struct {
	uri string
	acl types.ObjectCannedACL
}

func (act initAction) Run(ctx context.Context) error {
	r, err := helmutil.NewIndex().Reader()
	if err != nil {
		return errors.WithMessage(err, "get index reader")
	}

	storage := awss3.NewStorage()

	if err := storage.PutIndex(ctx, act.uri, act.acl, r); err != nil {
		return errors.WithMessage(err, "upload index to s3")
	}

	// TODO:
	// do we need to automatically do `helm repo add <name> <uri>`,
	// like we are doing `helm repo update` when we push a chart
	// with this plugin?

	return nil
}
