# TUCHOLD

This is a small static map app for exploring detention movement patterns from the Tucson INS Hold Room. It focuses on stays that begin in `2025` or `2026` and whose first facility is `TUCHOLD`, then shows how those stays move through later facilities or out to a departure country.

## Sources

- ACLU press release on ICE detention expansion plans in Colorado: https://www.aclu.org/press-releases/aclu-foia-litigation-reveals-new-information-about-plans-to-expand-ice-detention-in-colorado
- Deportation Data Project home: https://deportationdata.org/index.html
- ICE Detention Stays explorer: https://ice-detention-stays.apps.deportationdata.org/
- No Concentration Camps in Colorado: https://nocampscolorado.org/

The Deportation Data Project publishes processed ICE datasets and interactive tools based on records obtained from ICE through FOIA. This repo is a narrow, opinionated slice of that broader data universe, centered on Tucson-origin detention movement.

## Dataset Background

The included map is built from a detention stays parquet export, a separate `facility_coordinates.parquet` lookup for facility locations, and a small `reference_lookups.json` file for country coordinates and birth-region groupings. In this repo, a "stay" is one row with:

- a stay identifier
- a start timestamp
- a facility path encoded in `detention_facility_codes_all`
- release / charge / sentence summary fields
- an optional `departure_country`

Important limitation:

- this is stay-level data, not a fully exploded stint-level table
- exact time spent at every intermediate facility cannot be recovered from this source alone
- durations in the app are reliable for whole-stay timing and the first TUCHOLD holding interval, but not for every intermediate hop
