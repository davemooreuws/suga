package terraform

import (
	"context"
	"fmt"
	"sync"
)

// pluginCacheKey represents a unique identifier for a plugin
type pluginCacheKey struct {
	team    string
	library string
	version string
	name    string
	isIdentity bool // true for identity plugins, false for resource plugins
}

func newPluginCacheKey(plugin *Plugin, isIdentity bool) pluginCacheKey {
	return pluginCacheKey{
		team:       plugin.Library.Team,
		library:    plugin.Library.Name,
		version:    plugin.Library.Version,
		name:       plugin.Name,
		isIdentity: isIdentity,
	}
}

// PluginCache stores preloaded plugin manifests and implements PluginRepository
type PluginCache struct {
	resourcePlugins map[pluginCacheKey]*ResourcePluginManifest
	identityPlugins map[pluginCacheKey]*IdentityPluginManifest
	errors          map[pluginCacheKey]error
	mu              sync.RWMutex
}

// Ensure PluginCache implements PluginRepository interface
var _ PluginRepository = (*PluginCache)(nil)

// NewPluginCache creates a new plugin cache
func NewPluginCache() *PluginCache {
	return &PluginCache{
		resourcePlugins: make(map[pluginCacheKey]*ResourcePluginManifest),
		identityPlugins: make(map[pluginCacheKey]*IdentityPluginManifest),
		errors:          make(map[pluginCacheKey]error),
	}
}

// GetResourcePlugin returns a cached resource plugin manifest
func (c *PluginCache) GetResourcePlugin(team, library, version, name string) (*ResourcePluginManifest, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pluginCacheKey{team: team, library: library, version: version, name: name, isIdentity: false}

	if err, exists := c.errors[key]; exists {
		return nil, err
	}

	if plugin, exists := c.resourcePlugins[key]; exists {
		return plugin, nil
	}

	return nil, fmt.Errorf("plugin %s/%s/%s@%s not found in cache", team, library, name, version)
}

// GetIdentityPlugin returns a cached identity plugin manifest
func (c *PluginCache) GetIdentityPlugin(team, library, version, name string) (*IdentityPluginManifest, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pluginCacheKey{team: team, library: library, version: version, name: name, isIdentity: true}

	if err, exists := c.errors[key]; exists {
		return nil, err
	}

	if plugin, exists := c.identityPlugins[key]; exists {
		return plugin, nil
	}

	return nil, fmt.Errorf("identity plugin %s/%s/%s@%s not found in cache", team, library, name, version)
}

// setResourcePlugin stores a resource plugin in the cache
func (c *PluginCache) setResourcePlugin(key pluginCacheKey, plugin *ResourcePluginManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resourcePlugins[key] = plugin
}

// setIdentityPlugin stores an identity plugin in the cache
func (c *PluginCache) setIdentityPlugin(key pluginCacheKey, plugin *IdentityPluginManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.identityPlugins[key] = plugin
}

// setError stores an error in the cache
func (c *PluginCache) setError(key pluginCacheKey, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[key] = err
}

// PreloadPlugins concurrently loads all plugins for a platform spec
func (c *PluginCache) PreloadPlugins(ctx context.Context, platform *PlatformSpec, repository PluginRepository) error {
	plugins, err := platform.GetAllPlugins()
	if err != nil {
		return fmt.Errorf("failed to get plugins from platform spec: %w", err)
	}

	if len(plugins) == 0 {
		return nil
	}

	// Use a semaphore to limit concurrent requests to avoid overwhelming the API
	const maxConcurrency = 10
	semaphore := make(chan struct{}, maxConcurrency)

	var wg sync.WaitGroup

	// For each plugin, we need to determine if it's a resource or identity plugin
	// We'll try both and cache whichever succeeds
	for _, plugin := range plugins {
		wg.Add(1)
		go func(p *Plugin) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Try loading as resource plugin first
			resourceKey := newPluginCacheKey(p, false)
			resourcePlugin, resourceErr := repository.GetResourcePlugin(p.Library.Team, p.Library.Name, p.Library.Version, p.Name)
			if resourceErr == nil {
				c.setResourcePlugin(resourceKey, resourcePlugin)
			} else {
				c.setError(resourceKey, resourceErr)
			}

			// Also try loading as identity plugin
			identityKey := newPluginCacheKey(p, true)
			identityPlugin, identityErr := repository.GetIdentityPlugin(p.Library.Team, p.Library.Name, p.Library.Version, p.Name)
			if identityErr == nil {
				c.setIdentityPlugin(identityKey, identityPlugin)
			} else {
				c.setError(identityKey, identityErr)
			}
		}(plugin)
	}

	wg.Wait()

	return ctx.Err()
}