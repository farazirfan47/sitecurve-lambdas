type MonthlySearchVolume = {
    month: string;
    year: string;
    search_volume: number;
}

export type KeywordResult = {
    search_volume: number;
    cpc: number;
    competition: number;
    low_top_of_page_bid: number;
    high_top_of_page_bid: number;
    keyword_api_status: string;
    monthly_search_volume: MonthlySearchVolume[];
    keyword: string;
}