# Causal analysis of the elective surgical hub programme in England

## Description

This repository includes code used to run the The Health Foundation Improvement Analytics Unit's causal analysis of elective surgical hubs in England ([preprint here](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4888136)). Full details on background, methodology, and results can be found in the preprint.

## Data sources

Data comes from:
- [Hospital Episode Statistics](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics) which includes activity data from hospitals across England
- The Office for Health Improvement and Disparities' [NHS Acute (Hospital) Trust Catchment Populations dashboard](https://app.powerbi.com/view?r=eyJrIjoiODZmNGQ0YzItZDAwZi00MzFiLWE4NzAtMzVmNTUwMThmMTVlIiwidCI6ImVlNGUxNDk5LTRhMzUtNGIyZS1hZDQ3LTVmM2NmOWRlODY2NiIsImMiOjh9 ) which includes data on trust catchment characteristics and demographics
- Getting It Right First Time (GIRFT) Surgical Hubs team which provided information on surgical hub openeing dates and specialties

## Requirements

These scripts were written in R version 4.0.2 and RStudio Version 1.1.383. Analyses were completed using the [gsynth package](https://yiqingxu.org/packages/gsynth/) version 1.0.9. 

## How to use this repo

Scripts are run in order as numbered. Numbering is non-sequential as an artefact of varying attempts at correcting Gsynth outputs. The initial preamble scripts includes the packages needed to run the analyses. Scripts 07 and later assume a panel dataset of HES data with linked additional information from GIRFT - scripts on how to create this dataset to be added shortly. 

## Contributors

Authors: Melissa Co, Tatjana Marks, Freya Tracey, Stefano Conti, Geraldine Clarke

The Improvement Analytics Unit (IAU) is a partnership between NHS England and the Health Foundation. The research was undertaken by the IAU and jointly funded by the Health Foundation and NHS England. The HES data are used with permission from NHS England who retain the copyright for those data. The authors thank the Surgical Hubs team at Getting It Right First Time (NHS England) for providing advice on the context and interpretation of results, and colleagues in the IAU (the Health Foundation) for data management and preparation.

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit/).
