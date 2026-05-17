module github.com/plexusone/omniretrieve/providers/pgvector

go 1.25.5

require (
	github.com/lib/pq v1.10.9
	github.com/plexusone/omniretrieve v0.2.0
)

// For local development within the monorepo.
// This directive is ignored when the module is used as a dependency.
replace github.com/plexusone/omniretrieve => ../..
