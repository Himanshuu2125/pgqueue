// Package web embeds the admin UI templates and static assets.
package web

import "embed"

//go:embed templates static
var FS embed.FS
