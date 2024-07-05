export type Job = {
    type: string;
    id: string;
    keyword?: string;
}

export type JobGroup = {
    [key: string]: Job[];
}

export type DomainTagResult = {
    wesbite_types: string[];
    business_models: string[];
}

export type KeywordTagResult = {
    category: string;
    niche: string;
}

export type DomainResult = {
    _id: string;
    res: DomainTagResult;
}

export type KeywordResult = {
    _id: string;
    res: KeywordTagResult;
}