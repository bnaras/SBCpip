# SBCpip

`SBCpip` is an R package for processing Stanford Blood Center data in
order to feed it to the Platelet Inventory Prediction (`pip`)
package. The allows each site to build appropriate data processing
workflows suitable independently to use the `pip` package.

To install `SBCpip`, you need to install
[`pip`](https://github.com/bnaras/pip) first following instructions
given there. 

Then, install `SBCpip` as below.

```{r}
library(devtools)
devtools::install_github("bnaras/SBCpip")
```

