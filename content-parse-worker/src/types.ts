export type ContentParseJob = {
  id: string;
  url: string;
  page_id: string;
};

export type ParsedContentItem = {
  serp_id: string;
  page_meta: any;
  status: string;
}