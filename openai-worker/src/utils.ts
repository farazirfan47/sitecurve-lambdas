import { SQSRecord } from "aws-lambda";
import { Job, JobGroup } from "./types";
import { ObjectId } from "mongodb";

export const mergeBatchData = (records: SQSRecord[]) => {
  let mergedData: JobGroup = {};
  records.forEach((record) => {
    // Every record is a array
    let data = JSON.parse(record.body);
    data.forEach((job: Job) => {
      if (mergedData[job.type]) {
        mergedData[job.type].push(job);
      } else {
        mergedData[job.type] = [job];
      }
    });
  });
  return mergedData;
};

export const generateSerpPrompt = (pageMeta: any) => {
  let websiteMeta =
    "Title: " + pageMeta.title + "\n" + "Description: " + pageMeta.description;
  let prompt =
    "Analyse the following wesbite meta and determine website type and business model from the predefined list of types and business models";
  prompt += "\n\nWebsite Meta: \n" + websiteMeta;
  prompt +=
    "\n\nWebsite Types: Blogs,Corporate Websites,Directories,E-Commerce Sites,Educational Websites,Entertainment Websites,Financial Websites,Forums,Government Websites,Healthcare Websites,Landing Pages,Local Business Websites,News/Media Websites,Non-Profit and Charity Websites,Personal Websites,Portfolios,Review Sites,SaaS,Social Media,Travel and Tourism Websites,Wikis";
  prompt +=
    "\n\nBusiness Models: Affiliate Marketing,Consulting and Coaching,Digital Products,Display Ads,Donations,Dropshipping (E-com),Lead Generation,Marketplace,Online Courses and Education,Private Label (E-com),SaaS,Subscription Services,Other,Public Service - Goverment,Local Service Provider,Brick and Mortar";
  prompt +=
    '\n\nOutput the results in the following JSON format: {"website_types": "website types array", "business_models": "business models array"}';
  return prompt;
};

export const generateKeywordTagPrompt = (keyword: any) => {
  let prompt =
    "Analyse the following keyword and determine category and niche from the predefined list of categories and niches";
  prompt += "\n\nKeyword: " + keyword.keyword;
  prompt +=
    "\n\nCategories: health,travel,food & drink,finance,home services,technology,business,education,beauty,automotive,insurance,fashion,pets,legal,video games,real estate,sports,shopping,fitness,gambling,adult,entertainment,marketing,family,music,social issues,relationships,career,art,books,law enforcement,podcasts,lifestyle,television,religion,government,charity,cannabis,weddings,jewelry,hobbies,design,firearms,events,gardening,social media,military,engineering,media,aviation,science,movies,architecture,toys & games,hospitality,news,tobacco,security,agriculture,politics,funerals,games,comics";
  prompt +=
    "\n\nNiches: healthcare providers,restaurants,technology,investments,nutrition,dental care,travel,home services,beauty services,colleges,software,mental health,business software,credit cards,online education,recipes,automotive,hotels,mobile apps,skin care,cars,menswear,fitness programs,real estate,health insurance,dog food,bank accounts,loans,graduate programs,shopping destinations,web development,education,medicine,pet care,mattresses,sports betting,mobile phones,marketing,video games,entertainment,schools,business services,mobile games,legal,personal injury law,medical degrees,furniture,beauty products,law firms,family law,parenting,gyms,cruises,savings accounts,liquor,online gambling,suv,fitness gear,hair care,mortgages,e-commerce,insurance,car insurance,food & drink,accommodations,books,internet,bars,dating,women's health,cybersecurity,coffee,moving services,currency exchange,cryptocurrency,home insurance,roofing,beverages,hiking trails,movies,eye care,action rpg video games,windows,marketing agencies,business,shopping deals,plumbing services,travel agencies,bedding,beer,finance,appliances,nursing programs,mba programs,desserts,web hosting,jewelry,flooring,snacks,womenswear,cleaning products,pest control,professional development,car rentals,online marketplaces,nightlife venues,music,property management,career opportunities,utilities,luxury cars,grocery shopping,footwear,gardening,laptops,activewear,adult content,children's educational apps,gifts,photography,arts and crafts,seafood,wine,skincare,pickup trucks,cooking,music production software,design,pets,cleaning services,apartments,business banking,language learning apps,music playlists,fashion,slot machines,retirement planning,tires,logistics,detoxification,sports cars,special education schools,diamond jewelry,life insurance,charitable organizations,lingerie,law programs,senior living,casino games,finance software,computers,events,pet insurance,real estate agents,online payment platforms,criminal law,employment law,outdoor structures,cat supplies,family outings,online games,strategy video games,mobile plans,meal delivery,online advertising,marketing software,charitable donations,card games,trade schools,video editing software,cannabis dispensaries,first-person shooters,immigration law,graphic design,religious texts and scriptures,hunting gear,writing,recruitment,baby products,career and professional development,exam preparation,fantasy football,relationships,cocktails,airlines,dog training,anime,pet supplies,adult,tax software,religious organizations,language learning,office supplies,travel insurance,television,sports video games,pizza,real estate investing,astrology,electric vehicles,condiments,entrepreneurship,fragrances,gardening resources,motorcycles,resorts,performing arts,home security systems,outdoor activities,business phone systems,water filters,college majors,podcasts,mobile accessories,budget cars,business insurance,branding,baking,ski resorts,football,swimwear,employee feedback,wedding planning,real estate apps,medical devices,casino hotels,finance certifications,music lessons,eyewear,military careers,rental properties,digital marketing,streaming television,paint,classic cars,beef,golf courses,snowboarding,watches,firearms,employee benefits,architectural design,law schools,franchises,email marketing,wedding venues,cannabis strains,engineering exam preparation,toys & games,simulation video games,fertility treatments,credit scores,religious media,hvac services,beach destinations,basketball,handbags,real estate marketing,social media engagement,pool building,programming books,smoking accessories,social networking,ice cream,dog health supplements,flight training,personal care,bathrooms,sedans,art and design schools,printing equipment,cloud storage,football players,apparel,pet food,firearms accessories,customer service,documentary films,seo,social media profiles,streaming services,baseball,handguns,thriller movies,science,job application,hospitality,lottery tickets,towing services,poetry,audio equipment,email,running events,freight companies,marriage counseling,hair removal,frozen meals,business equipment,computer science books,magazines,shopping,engineering careers,sports,photography albums,websites,weight loss,tablets,tourist attractions,golf equipment,bands,sales,social media management,credit repair,jobs,engineering,accessories,financial services,project management,vehicle insurance,comedy specials,parenting apps,cannabis,racing video games,custom jewelry,jewelry stores,gambling,hybrid vehicles,music schools,grilling,golf apparel,music performances,leadership,freelancing,mechanical engineering,television specials,fruits,cameras,photography software,online poker,fantasy sports,fishing,tv shows,content marketing,virtual reality games,sexual health,religion,vans,legal software,music equipment,employment,newspapers,news,typography,funeral services,wood finishing,window treatments,side by side vehicles,fast food,meal planning,meat,retirement destinations,aviation,science fiction and fantasy books,military,wedding photography,politics,sleep manifestation,budgeting,pool cleaning equipment,science equipment,literature,file sharing,wildlife destinations,basketball players,soccer,hip hop,secondhand fashion,outerwear,loungewear,aircraft models,antiques,wedding invitations,event venues,government programs,online art galleries,collectibles,medical fellowships,tax deductions,kitchenware,rv accessories,construction equipment,wearable technology,energy drinks,airports,hunting,music festivals,beard care,celebrations,pc games,personal security,soil amendments,baby formula,pawn shops,diesel trucks,programming bootcamps,documentaries,virtual reality,martial arts,college football,golf,dog breeds,law enforcement,cannabis products,veterinary schools,real estate law,tequila,sandwiches,bikes,children's clothing,childcare,shopping apps,environmental conservation,home security,investment apps,painting,trucking,children's activities,task management,smoothies,breakfast foods,water sports gear,supply chain management,theater seating,genealogy,lighting,storage,televisions,smart home security systems,appetizers,music websites,wedding music,digestive health,physical therapy,cosmetic surgery,psychology graduate programs,poker,public speaking,writing skills,presidential history,transportation,college sports,music venues,music promotion,shotguns,affiliate marketing,social media,e-commerce marketing,human resources,climate change information,board games,lifestyle blogs,adventure video games,survival video games,farm management,flowers,health and wellness,first aid supplies,cosmetic dentistry,real estate courses,short stories,estate planning,law,vegan food,soccer players,hockey,tennis,music videos,music streaming,data visualization,security software,marriage,beauty,military bases,bioinformatics,wedding speeches,brand partnerships,engineering software,comic book publishers,massage,waterproofing,mathematics books,libraries,history books,tea,bakery,family travel,fantasy basketball,swimming,fishing gear,surfing gear,professional services,military gear,real estate financing,architecture education,market research,outdoor advertising,brand advertising,party games,political campaigns,lgbtq community,crop selection,government careers,medicare advantage plans,men's health,insulation,laundry,boats,blogging,artificial intelligence,dairy alternatives,rum,boxing,dirt bikes,hiking gear,music apps,music production,cat breeds,pet health supplements,biotech companies,children's media,international dating,renewable energy,nightlife,civil engineering,spa services,romantic comedy anime,online art classes,foot care,credit unions,religious music,waste disposal,film schools,web browsers,seo tools,wine tours,cycling gear,cheerleading,wrestling,auto racing,volleyball,music podcasts,music licensing,fish food,security cameras,business bank accounts,employee experience,comics,jewelry insurance,amusement parks,architecture publications,emergency preparedness,psychic readings,quotes,esports,mmorpg,political magazines,hospitality education,television commercials,farm equipment,plants,casino rewards programs,credit monitoring,investment banks,tools,drama,telecommunications,photo editing software,video calling,internet privacy,intellectual property law,national parks,educational apps,bowling,rodeo,college basketball,mixed martial arts,snowmobiles,music video games,music events,backpacks,security equipment,international business,blogs,accident insurance,boat insurance,dental insurance,renters insurance,voice acting,anti-aging skincare,technology for law enforcement,tabletop role-playing games,social media advertising,apps,memorable experiences,dance styles,career advice,magic practitioners,political advertising,professional relationships,satellite television,bankruptcy,essays,bankruptcy law,retreat centers,relocation,historical sites,speedcubes,billiards,horse racing,rugby,skateboarding,music awards,fashion schools,dresses,dog grooming,pricing,coworking spaces,manufacturing,security services,nonprofit organizations,team building,venture capital,inventory management,time tracking,phone insurance,veteran charities,wedding decor,word games,tattoos,tarot readings,political podcasts,electrical engineering,materials engineering,homelessness,agricultural markets,friendship and relationships,hobbies,culinary schools,statistics,writing tools,life philosophy,scanning technology,web design services,alcohol consumption,disc golf,equestrian sports,softball,product management,professional networking,legal services,procurement,organizational structure,business processes,payroll,employee recognition,employee retention,webinars,science books,event insurance,maternity services,reloading supplies,dice games,local advertising,event marketing,ott advertising,health supplements,japanese-themed tattoos,tattoo fonts,motivational speakers,gaming,political documentaries,political speeches,social work,computational fluid dynamics (cfd),chemical engineering,structural engineering,vacation rental management software,journalism,domestic violence support,social issues,color theory,agricultural technology,crop protection,livestock,digital comics,comic book characters,government contracts,food and dining,clipart,airsoft guns,model railroading,autographs,media production";
  prompt +=
    '\n\nOutput the results in the following JSON format: {"category": "category", "niche": "niche"}';
  return prompt;
};

export const tagSerpDomains = async (mongoClient: any, domainResults: any) => {
  const db = mongoClient.db("sitecurve");
  const serps = db.collection("keywords");
  // Update them in bulk using bulkWrite, here is the collection structure
  // keywords has following keys _id, keyword, search_volume, serps - serps is a array of objects with keys _id, page_meta, website_types, business_models, so we have to search serps array and update the website_types and business_models
  let bulkOps: any = [];
  for (let domainResult of domainResults) {
    bulkOps.push({
      updateOne: {
        filter: { "serps._id": domainResult._id },
        update: {
          $set: {
            "serps.$[elem].website_types": domainResult.res.website_types,
            "serps.$[elem].business_models": domainResult.res.business_models,
            "serps.$[elem].openai_tag_status": "DONE",
          },
        },
        arrayFilters: [{ "elem._id": domainResult._id }],
        upsert: true,
      },
    });
  }
  await serps.bulkWrite(bulkOps);
  console.log("Domains updated in MongoDB");
};

export const saveTaggedKeywords = async (
  mongoClient: any,
  keywordResults: any
) => {
  const db = mongoClient.db("sitecurve");
  const keywords = db.collection("keywords");
  let bulkOps: any = [];
  for (let keywordResult of keywordResults) {
    bulkOps.push({
      updateOne: {
        filter: { _id: new ObjectId(keywordResult._id)},
        update: {
          $set: {
            category: keywordResult.res.category,
            niche: keywordResult.res.niche,
            openai_tag_status: "DONE",
          },
        },
        upsert: true,
      },
    });
  }
  await keywords.bulkWrite(bulkOps);
  console.log("Keywords updated in MongoDB");
};

// export const tagSerpDomain = async (mongoClient: any, serpId: string, res: any) => {
//     const db = mongoClient.db("sitecurve");
//     const serps = db.collection("keywords");
//     await serps.updateOne(
//         { _id: serpId },
//         { $set: { website_types: res.websiteTypes, business_models: res.businessModels } }
//     );
// };
