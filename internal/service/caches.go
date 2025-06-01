package service

import (
	"github.com/InsulaLabs/insi/config"
	"github.com/jellydator/ttlcache/v3"
)

func initLocalCaches(cacheConfig *config.Cache) (*localCaches, error) {

	/*
		Setuo the actual caches

	*/
	lc := &localCaches{
		apiKeys: ttlcache.New(
			ttlcache.WithTTL[string, string](cacheConfig.Keys),
			ttlcache.WithDisableTouchOnHit[string, string](), // dont bump ttl on hit
		),
	}

	/*
		Start the caches

		These need to live for the lifetime of the service (the entire process)
		so passing a context for cancel is stupid
	*/
	go lc.apiKeys.Start()

	return lc, nil
}
