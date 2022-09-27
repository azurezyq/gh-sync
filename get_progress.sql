SELECT id, FORMAT_TIMESTAMP('%Y-%m-%dT%XZ', updatedAt, 'UTC') as updatedAt FROM `pingcap-gardener.github.pull_requests_exp` WHERE author is not NULL
