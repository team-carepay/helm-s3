package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"

	"github.com/pkg/errors"

	"github.com/hypnoglow/helm-s3/internal/awss3"
	"github.com/hypnoglow/helm-s3/internal/helmutil"
)

type reindexAction struct {
	repoName string
	acl types.ObjectCannedACL
	relative bool
}

func (act reindexAction) Run(ctx context.Context) error {
	repoEntry, err := helmutil.LookupRepoEntry(act.repoName)
	if err != nil {
		return err
	}

	storage := awss3.NewStorage()

	items, errs := storage.Traverse(ctx, repoEntry.URL())

	builtIndex := make(chan helmutil.Index, 1)
	go func() {
		idx := helmutil.NewIndex()
		for item := range items {
			baseURL := repoEntry.URL()
			if act.relative {
				baseURL = ""
			}
			if err := idx.Add(item.Meta.Value(), item.Filename, baseURL, item.Hash); err != nil {
				log.Printf("[ERROR] failed to add chart to the index: %s", err)
			}
		}
		idx.SortEntries()

		builtIndex <- idx
	}()

	for err = range errs {
		return errors.Wrap(err, "traverse the chart repository")
	}

	idx := <-builtIndex

	r, err := idx.Reader()
	if err != nil {
		return errors.Wrap(err, "get index reader")
	}

	if err := storage.PutIndex(ctx, repoEntry.URL(), act.acl, r); err != nil {
		return errors.Wrap(err, "upload index to the repository")
	}

	if err := idx.WriteFile(repoEntry.CacheFile(), 0644); err != nil {
		return errors.WithMessage(err, "update local index")
	}

	return nil
}
