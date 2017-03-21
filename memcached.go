package memcached

import (
	"strings"

	"github.com/golang-plus/caching"
	"github.com/golang-plus/errors"

	"github.com/rainycape/memcache"
)

type Container struct {
	client *memcache.Client
}

func (c *Container) Clear() error {
	err := c.client.Flush(0)
	if err != nil {
		return errors.Wrap(err, "could not clear container")
	}

	return nil
}

func (c *Container) Remove(key string) error {
	err := c.client.Delete(key)
	if err != nil && err != memcache.ErrCacheMiss {
		return errors.Wrapf(err, "could not remove item with key %q from container", key)
	}

	return nil
}

func (c *Container) Put(item *caching.Item) error {
	data, err := item.Marshal()
	if err != nil {
		return err
	}

	mci := &memcache.Item{
		Key:   item.Key,
		Value: data,
	}

	err = c.client.Set(mci)
	if err != nil {
		return errors.Wrapf(err, "could not put (insert/update) item with key %q to container", item.Key)
	}

	return nil
}

func (c *Container) Get(key string) (*caching.Item, error) {
	mci, err := c.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "could not get item with key %s from container", key)
	}

	var item *caching.Item

	err = item.Unmarshal(mci.Value)
	if err != nil {
		return nil, err
	}

	return item, nil
}

// NewContainer returns a new memcached cache container.
func NewContainer(servers ...string) (caching.Container, error) {
	client, err := memcache.New(servers...)
	if err != nil {
		return nil, errors.Wrapf(err, "could not connect to servers %q", strings.Join(servers, ","))
	}

	return &Container{
		client: client,
	}, nil
}

// Short to NewContainer func.
func New(servers ...string) (caching.Container, error) {
	return NewContainer(servers...)
}
