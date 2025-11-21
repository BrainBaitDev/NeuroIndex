use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Geographic coordinate (latitude, longitude)
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f64, // Latitude: -90 to 90
    pub lon: f64, // Longitude: -180 to 180
}

impl GeoPoint {
    /// Create a new geographic point
    pub fn new(lat: f64, lon: f64) -> Result<Self, String> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(format!("Invalid latitude: {} (must be -90 to 90)", lat));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(format!("Invalid longitude: {} (must be -180 to 180)", lon));
        }
        Ok(Self { lat, lon })
    }

    /// Calculate distance to another point using Haversine formula (in meters)
    pub fn distance_to(&self, other: &GeoPoint) -> f64 {
        const EARTH_RADIUS_M: f64 = 6_371_000.0; // Earth radius in meters

        let lat1_rad = self.lat.to_radians();
        let lat2_rad = other.lat.to_radians();
        let delta_lat = (other.lat - self.lat).to_radians();
        let delta_lon = (other.lon - self.lon).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);

        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_M * c
    }

    /// Encode point to geohash with specified precision (1-12 characters)
    pub fn to_geohash(&self, precision: usize) -> String {
        encode_geohash(self.lat, self.lon, precision)
    }

    /// Check if point is within bounding box
    pub fn in_bbox(&self, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> bool {
        self.lat >= min_lat && self.lat <= max_lat && self.lon >= min_lon && self.lon <= max_lon
    }
}

/// Geospatial index for efficient location-based queries
pub struct GeoIndex<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    /// Map from geohash prefix to set of keys
    /// Using precision 6 (~0.6km cells) for efficient radius queries
    index: Arc<RwLock<HashMap<String, HashSet<K>>>>,

    /// Map from key to its geographic point
    locations: Arc<RwLock<HashMap<K, GeoPoint>>>,

    /// Geohash precision (default: 6)
    precision: usize,

    /// Index name
    name: String,
}

impl<K> GeoIndex<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    /// Create a new geospatial index with default precision (6)
    pub fn new(name: String) -> Self {
        Self::with_precision(name, 6)
    }

    /// Create a new geospatial index with custom precision
    /// Precision levels:
    /// - 1: ±2500 km
    /// - 2: ±630 km
    /// - 3: ±78 km
    /// - 4: ±20 km
    /// - 5: ±2.4 km
    /// - 6: ±0.61 km (default)
    /// - 7: ±0.076 km
    /// - 8: ±0.019 km
    pub fn with_precision(name: String, precision: usize) -> Self {
        assert!(
            (1..=12).contains(&precision),
            "Geohash precision must be 1-12"
        );
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
            locations: Arc::new(RwLock::new(HashMap::new())),
            precision,
            name,
        }
    }

    /// Add a point to the index
    pub fn add(&self, key: K, point: GeoPoint) {
        let geohash = point.to_geohash(self.precision);

        // Add to geohash index
        let mut index = self.index.write().unwrap();
        index
            .entry(geohash)
            .or_insert_with(HashSet::new)
            .insert(key.clone());

        // Store location
        let mut locations = self.locations.write().unwrap();
        locations.insert(key, point);
    }

    /// Remove a point from the index
    pub fn remove(&self, key: &K) {
        let mut locations = self.locations.write().unwrap();
        if let Some(point) = locations.remove(key) {
            let geohash = point.to_geohash(self.precision);

            let mut index = self.index.write().unwrap();
            if let Some(keys) = index.get_mut(&geohash) {
                keys.remove(key);
                if keys.is_empty() {
                    index.remove(&geohash);
                }
            }
        }
    }

    /// Query points within a radius (in meters) of a center point
    pub fn query_radius(&self, center: GeoPoint, radius_m: f64) -> Vec<K> {
        let locations = self.locations.read().unwrap();

        // For larger radii, scan all points (brute force but accurate)
        // Geohash optimization works best for small, local queries
        if radius_m > 50_000.0 {
            // > 50km: brute force
            return locations
                .iter()
                .filter(|(_, point)| center.distance_to(point) <= radius_m)
                .map(|(key, _)| key.clone())
                .collect();
        }

        // Get candidate geohashes that might overlap with radius
        let candidate_hashes = get_neighbor_geohashes(&center, radius_m, self.precision);

        let mut results = Vec::new();
        let index = self.index.read().unwrap();

        for hash in candidate_hashes {
            if let Some(keys) = index.get(&hash) {
                for key in keys {
                    if let Some(point) = locations.get(key) {
                        let distance = center.distance_to(point);
                        if distance <= radius_m {
                            results.push(key.clone());
                        }
                    }
                }
            }
        }

        results
    }

    /// Query points within a bounding box
    pub fn query_bbox(&self, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Vec<K> {
        let locations = self.locations.read().unwrap();

        locations
            .iter()
            .filter(|(_, point)| point.in_bbox(min_lat, min_lon, max_lat, max_lon))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Find k-nearest neighbors to a point
    pub fn query_nearest(&self, center: GeoPoint, k: usize) -> Vec<(K, f64)> {
        let locations = self.locations.read().unwrap();

        let mut distances: Vec<(K, f64)> = locations
            .iter()
            .map(|(key, point)| (key.clone(), center.distance_to(point)))
            .collect();

        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        distances.truncate(k);

        distances
    }

    /// Get the location of a key
    pub fn get_location(&self, key: &K) -> Option<GeoPoint> {
        let locations = self.locations.read().unwrap();
        locations.get(key).copied()
    }

    /// Get index statistics
    pub fn stats(&self) -> GeoIndexStats {
        let index = self.index.read().unwrap();
        let locations = self.locations.read().unwrap();

        GeoIndexStats {
            name: self.name.clone(),
            total_points: locations.len(),
            geohash_cells: index.len(),
            precision: self.precision,
        }
    }

    /// Get index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Clear the index
    pub fn clear(&self) {
        let mut index = self.index.write().unwrap();
        let mut locations = self.locations.write().unwrap();
        index.clear();
        locations.clear();
    }
}

/// Geospatial index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoIndexStats {
    pub name: String,
    pub total_points: usize,
    pub geohash_cells: usize,
    pub precision: usize,
}

/// Manager for multiple geospatial indexes
pub struct GeoIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    indexes: Arc<RwLock<HashMap<String, GeoIndex<K>>>>,
}

impl<K> GeoIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    /// Create a new geospatial index manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new geospatial index
    pub fn create_index(&self, name: String, precision: usize) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.insert(name.clone(), GeoIndex::with_precision(name, precision));
    }

    /// Drop a geospatial index
    pub fn drop_index(&self, name: &str) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.remove(name);
    }

    /// Add a point to an index
    pub fn add_to_index(&self, index_name: &str, key: K, point: GeoPoint) {
        let indexes = self.indexes.read().unwrap();
        if let Some(index) = indexes.get(index_name) {
            index.add(key, point);
        }
    }

    /// Remove a point from an index
    pub fn remove_from_index(&self, index_name: &str, key: &K) {
        let indexes = self.indexes.read().unwrap();
        if let Some(index) = indexes.get(index_name) {
            index.remove(key);
        }
    }

    /// Query radius in an index
    pub fn query_radius(&self, index_name: &str, center: GeoPoint, radius_m: f64) -> Vec<K> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(index_name)
            .map(|idx| idx.query_radius(center, radius_m))
            .unwrap_or_default()
    }

    /// Query bounding box in an index
    pub fn query_bbox(
        &self,
        index_name: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Vec<K> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(index_name)
            .map(|idx| idx.query_bbox(min_lat, min_lon, max_lat, max_lon))
            .unwrap_or_default()
    }

    /// Query k-nearest neighbors in an index
    pub fn query_nearest(&self, index_name: &str, center: GeoPoint, k: usize) -> Vec<(K, f64)> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(index_name)
            .map(|idx| idx.query_nearest(center, k))
            .unwrap_or_default()
    }

    /// Get location of a key in an index
    pub fn get_location(&self, index_name: &str, key: &K) -> Option<GeoPoint> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(index_name)
            .and_then(|idx| idx.get_location(key))
    }

    /// List all index names
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read().unwrap();
        indexes.keys().cloned().collect()
    }

    /// Get index statistics
    pub fn index_stats(&self, index_name: &str) -> Option<GeoIndexStats> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(index_name).map(|idx| idx.stats())
    }
}

impl<K> Default for GeoIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Geohash Implementation
// ============================================================================

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Encode latitude and longitude to geohash
fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut hash = String::with_capacity(precision);
    let mut bits = 0u8;
    let mut bit_count = 0;

    for _ in 0..precision * 5 {
        // 5 bits per character
        if bit_count % 2 == 0 {
            // Even: longitude
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits |= 1 << (4 - (bit_count % 5));
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            // Odd: latitude
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits |= 1 << (4 - (bit_count % 5));
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }

        bit_count += 1;

        if bit_count % 5 == 0 {
            hash.push(BASE32[bits as usize] as char);
            bits = 0;
        }
    }

    hash
}

/// Get neighbor geohashes that might overlap with a radius query
fn get_neighbor_geohashes(center: &GeoPoint, radius_m: f64, precision: usize) -> Vec<String> {
    // Include center geohash and neighbors based on radius
    let center_hash = center.to_geohash(precision);

    // Calculate offset in degrees based on radius
    // At equator: 1 degree ≈ 111km, so offset = radius_km / 111
    // Add 50% margin for safety across all geohash cells
    let radius_km = radius_m / 1000.0;
    let offset_degrees = ((radius_km / 111.0) * 1.5).max(0.01); // Minimum 0.01 degrees

    let mut hashes = HashSet::new();
    hashes.insert(center_hash);

    // Sample points in a grid around center to cover radius
    let steps = 5; // 5x5 grid for better coverage
    let step_size = offset_degrees / steps as f64;

    for i in -(steps as i32)..=(steps as i32) {
        for j in -(steps as i32)..=(steps as i32) {
            let lat_offset = i as f64 * step_size;
            let lon_offset = j as f64 * step_size;

            if let Ok(neighbor) = GeoPoint::new(
                (center.lat + lat_offset).clamp(-90.0, 90.0),
                (center.lon + lon_offset).clamp(-180.0, 180.0),
            ) {
                hashes.insert(neighbor.to_geohash(precision));
            }
        }
    }

    hashes.into_iter().collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geopoint_creation() {
        let p = GeoPoint::new(40.7128, -74.0060).unwrap(); // New York
        assert_eq!(p.lat, 40.7128);
        assert_eq!(p.lon, -74.0060);

        assert!(GeoPoint::new(100.0, 0.0).is_err()); // Invalid lat
        assert!(GeoPoint::new(0.0, 200.0).is_err()); // Invalid lon
    }

    #[test]
    fn test_haversine_distance() {
        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let london = GeoPoint::new(51.5074, -0.1278).unwrap();

        let distance = nyc.distance_to(&london);
        // Distance should be approximately 5,585 km
        assert!((distance - 5_585_000.0).abs() < 10_000.0);
    }

    #[test]
    fn test_geohash_encoding() {
        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let hash = nyc.to_geohash(6);
        assert_eq!(hash.len(), 6);
        assert!(hash.starts_with("dr5r")); // NYC geohash starts with dr5r
    }

    #[test]
    fn test_geo_index_add_remove() {
        let index = GeoIndex::<String>::new("locations".to_string());

        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        index.add("nyc".to_string(), nyc);

        assert_eq!(index.get_location(&"nyc".to_string()), Some(nyc));

        index.remove(&"nyc".to_string());
        assert_eq!(index.get_location(&"nyc".to_string()), None);
    }

    #[test]
    fn test_radius_query() {
        let index = GeoIndex::<String>::new("locations".to_string());

        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let brooklyn = GeoPoint::new(40.6782, -73.9442).unwrap();
        let boston = GeoPoint::new(42.3601, -71.0589).unwrap();

        index.add("nyc".to_string(), nyc);
        index.add("brooklyn".to_string(), brooklyn);
        index.add("boston".to_string(), boston);

        // Query 20km radius from NYC (should include Brooklyn, not Boston)
        let results = index.query_radius(nyc, 20_000.0);
        assert!(results.contains(&"nyc".to_string()));
        assert!(results.contains(&"brooklyn".to_string()));
        assert!(!results.contains(&"boston".to_string()));
    }

    #[test]
    fn test_bbox_query() {
        let index = GeoIndex::<String>::new("locations".to_string());

        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let london = GeoPoint::new(51.5074, -0.1278).unwrap();

        index.add("nyc".to_string(), nyc);
        index.add("london".to_string(), london);

        // Bbox covering NYC area
        let results = index.query_bbox(40.0, -75.0, 41.0, -73.0);
        assert!(results.contains(&"nyc".to_string()));
        assert!(!results.contains(&"london".to_string()));
    }

    #[test]
    fn test_nearest_neighbors() {
        let index = GeoIndex::<String>::new("locations".to_string());

        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let brooklyn = GeoPoint::new(40.6782, -73.9442).unwrap();
        let boston = GeoPoint::new(42.3601, -71.0589).unwrap();

        index.add("nyc".to_string(), nyc);
        index.add("brooklyn".to_string(), brooklyn);
        index.add("boston".to_string(), boston);

        // Find 2 nearest to NYC
        let nearest = index.query_nearest(nyc, 2);
        assert_eq!(nearest.len(), 2);
        assert_eq!(nearest[0].0, "nyc".to_string()); // Closest to itself
        assert_eq!(nearest[1].0, "brooklyn".to_string()); // Brooklyn is closer than Boston
    }
}
