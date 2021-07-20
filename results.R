library(tidyverse)
library(knitr)

url_paths <- list.files(path="./results", 
           pattern="urlcounts.csv", 
           full.names=T,
           recursive=T)

get_dataframe <- function(path) {
  name <- str_split(path, "/")[[1]][3]
  df <- read_csv(path)
  df <- df %>% 
    mutate(site=name)
  
  return(df)
}

url_counts <- lapply(url_paths, get_dataframe) %>% 
  bind_rows()

url_counts %>% 
  filter(site!="journalgazette") %>% 
  group_by(site) %>% 
  summarize(pct=urls/max(urls)) %>% 
  filter(pct!=1) %>% 
  ungroup() %>% 
  summarize(mean=mean(pct),
            max=max(pct),
            min=min(pct))

url_counts %>% 
  pivot_wider(names_from=site, values_from=urls) %>% 
  kable("latex")

url_counts %>% 
  filter(service!="onsite") %>% 
  group_by(site) %>% 
  mutate(rank=dense_rank(urls)) %>% 
  group_by(service) %>% 
  summarize(mean(rank))
