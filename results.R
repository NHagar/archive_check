library(tidyverse)
library(knitr)

# ----URL counts----
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

# ----Headline counts----
headline_paths <- list.files(path="./results", 
                        pattern="headlinecounts.csv", 
                        full.names=T,
                        recursive=T)

hed_counts <- lapply(headline_paths, get_dataframe) %>% 
  bind_rows()

hed_pcts <- hed_counts %>% 
  mutate(pct_biden=biden_count / (biden_count + trump_count)) %>% 
  group_by(site) %>% 
  arrange(pct_biden) 

heds_onsite <- hed_pcts %>% filter(service=="onsite")

hed_pcts %>% 
  ggplot(aes(service, pct_biden)) + 
  geom_hline(yintercept=0.5) + 
  geom_bar(stat="identity") + 
  geom_bar(data=heds_onsite, 
           aes(x=service, y=pct_biden), 
           fill="gold",
           stat="identity") + 
  facet_wrap(vars(site)) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))

# ----LDA----
lda_paths <- list.files(path="./results", 
                             pattern="lda.csv", 
                             full.names=T,
                             recursive=T)

lda <- lapply(lda_paths, get_dataframe) %>% 
  bind_rows()

lda %>% 
  select(-words) %>% 
  pivot_wider(-coherence, names_from=site, values_from=k) %>% 
  kable("latex")
