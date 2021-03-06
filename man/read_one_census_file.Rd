% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/process-data.R
\name{read_one_census_file}
\alias{read_one_census_file}
\title{Read a single census file data and return it, as is}
\usage{
read_one_census_file(filename, locations)
}
\arguments{
\item{filename}{the fully qualified path of the file}

\item{locations}{a character vector locations of interest}
}
\value{
a list of four items, filename, raw_data (tibble), report a
    list consisting of summary tibble, census_data (tibble)
}
\description{
Read a single census file data and return it, as is
}
