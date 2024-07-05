import { ObjectId } from "mongodb";

export type SerpRow = {
    landscape_id: string;
    keyword_id: string;
    rank_group: number;
    rank_absolute: number;
    title: string;
    description: string;
    url: string;
    breadcrumb: string;
    id: ObjectId;
  };
  
export type ResultItem = {
    type: string,
    rank_group: number,
    rank_absolute: number,
    domain: string,
    title: string,
    description: string,
    url: string,
    breadcrumb: string
  }; 
  
  export type SerpTagJob = {
    // type: "keyword" | "serp",
    url: string,
    serp_id: string,
  };