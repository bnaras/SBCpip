# SBCpip

`SBCpip` is an R package for processing Stanford Blood Center
data. The data can then be fed to the [Platelet Inventory Prediction
(`pip`)](https://bnaras.github.io/pip) package. 

This separation allows each organization to build appropriate data
processing workflows for local data in site-specific packages and not
worry about the site-independent parts in the `pip` package.

To install `SBCpip`, you need to first install
[`pip`](https://github.com/bnaras/pip) first following instructions
given there.

Then, install `SBCpip` as below.

```{r}
library(devtools)
devtools::install_github("bnaras/SBCpip")
```

