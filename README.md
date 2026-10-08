# sg-hawker-centre
Dynamic analytics pipeline built in Databricks (Community Edition). Automates data extraction from the data.gov.sg API and geocodes spatial structures via OneMap API to map out hawker centre distributions.

# Singapore Hawker Density Lakehouse Pipeline
**Tech Stack:** Cloud Databricks (Community Edition), Python (Pandas/Requests), Live Government API (`data.gov.sg`), Spatial Geocoding (`OneMap API`), Folium

## The Story Behind the Map
I recently relocated from Malaysia to Singapore! As a data analyst (and a foodie!), I wanted this project to solve a very practical mission: **How can I use data to map out the country's hawker infrastructure and find the ultimate food hotspots (or hidden food deserts)?**

Instead of downloading a static spreadsheet, this project is built as a programmatic pipeline running inside a **Databricks cloud lakehouse environment**. It automatically hooks into live government servers, cleans up local address fields, and drops them into an interactive spatial dashboard.

## The Data Pipeline & Workflow

The architecture is split into a simple, automated backend flow:

1. **Live API Ingestion:** The script sends a dynamic request to the `data.gov.sg` datastore endpoint to fetch live records for all registered government markets and hawker centers.
2. **Postal Standardization:** Cleans the raw data strings and applies proper 6-digit leading-zero padding to ensure all Singapore postal codes are formatted correctly.
3. **Geocoding Engine:** Loops through the unique postal codes to query the free **Singapore Land Authority (OneMap) API**, instantly translating flat addresses into exact GPS coordinates (Latitude and Longitude).
4. **Interactive Mapping:** Groups the coordinates by planning sectors and uses **Folium** to plot individual markers and density heat-layers directly inside the notebook.

## Project Structure
* `/notebooks` - Full Python/Pandas codebase formatted for Databricks.
* `/output` - Self-contained interactive `.html` map file (downloadable to view in-browser).

## Optional credentials
This is a public portfolio repo. Do not commit API keys.

The map runs with **OpenStreetMap** tiles by default (no key). To use watermark-free Carto Voyager rasters, set `CARTO_TILE_KEY` in the environment or a Databricks widget named `carto_tile_key`. An optional OneMap token can be set the same way (`ONEMAP_TOKEN` / `onemap_token`). Copy `.env.example` to `.env` for local runs.

If you generate `output/hawker_density_map.html` with a Carto key, that HTML will contain the key — do not commit it.

*Jom makan!*
