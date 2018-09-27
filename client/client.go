package client

import (
	"context"
	"github.com/srikrsna/flock/protos"
)

type Logger interface {
	Printf(string, ...interface{})
}

type Client struct {
	Cli flock.FlockClient
	Log Logger
}

// Flock migrates the given schema to the new database
func (c Client) Flock(ctx context.Context, schema string) error {
	cli, err := c.Cli.Flock(ctx)
	if err != nil {
		return err
	}

	if err := cli.Send(&flock.FlockRequest{Value: &flock.FlockRequest_Ping{}}); err != nil {
		return err
	}

	if _, err := cli.Recv(); err != nil {
		return err
	}

	return nil
}
