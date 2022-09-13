package index

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-timings"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"os"
)

type IndexTablesOptions struct {
	Database sql.Database
	Tables   []sql.Table
	Monitor  timings.Monitor
	Logger   *log.Logger
}

func IndexTables(ctx context.Context, opts *IndexTablesOptions, iterator_uri string, to_iterate ...string) error {

	iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		_, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return fmt.Errorf("Failed to parse URI for %s, %w", path, err)
		}

		body, err := io.ReadAll(r)

		if err != nil {
			return fmt.Errorf("Failed to read body for %s, %w", path, err)
		}

		opts.Database.Lock()
		defer opts.Database.Unlock()

		var alt *uri.AltGeom

		if uri_args.IsAlternate {
			alt = uri_args.AltGeom
		}

		err = opts.Database.IndexFeature(ctx, opts.Tables, body, alt)

		if err != nil {
			return fmt.Errorf("Failed to index %s, %w", path, err)
		}

		go opts.Monitor.Signal(ctx)
		return nil
	}

	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator because: %s", err)
	}

	opts.Monitor.Start(ctx, os.Stdout)
	defer opts.Monitor.Stop(ctx)

	err = iter.IterateURIs(ctx, to_iterate...)

	if err != nil {
		return fmt.Errorf("Failed to index paths in %s mode because: %s", iterator_uri, err)
	}

	return nil
}
