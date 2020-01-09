library(XML)
library(uuid)

# XML STRING 
empty_template.xml <- '<?xml version="1.0" encoding="utf-8"?>
<survey>
  <Description><![CDATA[]]></Description>
  <notes><![CDATA[]]></notes>
  <PrivateNotes><![CDATA[]]></PrivateNotes>
  <documentation Label="Documentation">
    <documentlinks />
  </documentation>
  <geoTypes>
  </geoTypes>
  <GeoSurveyDataset>
    <DataBibliographicInfo />
    <notes />
    <PrivateNotes><![CDATA[]]></PrivateNotes>
    <Description><![CDATA[]]></Description>
    <datasets>
    </datasets>
    <iterations />
    <tables>
    </tables>
  </GeoSurveyDataset>
  <SurveyDatasets>
  </SurveyDatasets>
  <Categories>
    <string>CED</string>
    <string>Population</string>
  </Categories>
</survey>'

# SAMPLE DATA FRAMES WITH INFO ABOUT VARIABLES AND GEO LEVELS
variablesDF <- read.csv('tables.csv', stringsAsFactors = FALSE)
geoLevelsDF <- read.csv('geos.csv', stringsAsFactors = FALSE, header = FALSE)

# BUILD XML TREE
doc = xmlTreeParse(empty_template.xml, useInternalNodes = T)  # PARSE STRING

addAttributes(xmlRoot(doc), GUID=UUIDgenerate(),  # Add attributes to the top node
                                Visible="true",
                                GeoTypeTreeNodeExpanded="true",
                                GeoCorrespondenceTreeNodeExpanded="false",
                                name="CED",
                                DisplayName="CED",
                                year="2019",
                                Categories="")

# ADD GEO TYPES
geoTypesNodes <- xpathApply(doc, '//geoTypes')  # find geo types tag

for (g in 1:nrow(geoLevelsDF)) {
  geoNode <- newXMLNode("geoType", parent=geoTypesNodes)
    addAttributes(geoNode, GUID=UUIDgenerate(),
                           MapDisplayScale="",
                           SearchPriorityCode="",
                           Name=geoLevelsDF[g,1],
                           Label=geoLevelsDF[g,2],
                           QLabel=geoLevelsDF[g,2],
                           AvailabilityNote="",
                           SourceFilesInfo="",
                           RelevantGeoIDs="FIPS,NAME,QName,NATION,PROVINCE,MUNICIPALITY,CT,REGION",
                           PluralName=geoLevelsDF[g,2],
                           fullCoverage="true",
                           majorGeo="true",
                           GeoAbrev="",
                           Indent=g-1,
                           Sumlev=substr(geoLevelsDF[g,1],3,6),
                           FipsCodeLength="2",
                           FipsCodeFieldName="FIPS",
                           FipsCodePartialFieldName=geoLevelsDF[g,2],
                           FipsCodePartialLength="0",
                           Notes="",
                           DocSectionLinks="")
  newXMLNode("Visible", 'true',parent=geoNode)
}


geo_ident_node <- xpathApply(doc, '//GeoSurveyDataset')  # FIND GeoSurveyDataset tag

addAttributes(geo_ident_node[[1]], GUID=UUIDgenerate(),  # Add attributes (GUID is generated with UUIDgenerate())
                                DocLinksAsString="",
                                SurveyDatasetTreeNodeExpanded="true",
                                TablesTreeNodeExpanded="true",
                                IterationsTreeNodeExpanded="false",
                                DatasetsTreeNodeExpanded="true",
                                description="",
                                ProductTags="",
                                Visible="false",
                                abbreviation="Geo",
                                source="",
                                publisher="",
                                name="Geography Summary File",
                                DisplayName="Geography Summary File")

geo_ident_tables <- xpathApply(doc, '//GeoSurveyDataset//tables')  # FIND tables node in GeoSurveyDataset tag

geo_ident_table <- newXMLNode("table", parent=geo_ident_tables)  # add new child element table to
                                                                    # //GeoSurveyDataset//tables
addAttributes(geo_ident_table, GUID=UUIDgenerate(),  # Add attributes (GUID is generated with UUIDgenerate())
                                VariablesAreExclusive="false",
                                notes="",
                                PrivateNotes="",
                                DollarYear="0",
                                PercentBaseMin="1",
                                name="G001",
                                displayName="G1.",
                                title="Geography Identifiers",
                                titleWrapped="Geography Identifiers",
                                titleShort="",
                                universe="none",
                                Visible="false",
                                TreeNodeCollapsed="true",
                                DocSectionLinks="",
                                DataCategories="",
                                ProductTags="",
                                FilterRuleName="",
                                CategoryPriorityOrder="0",
                                ShowOnFirstPageOfCategoryListing="false",
                                DbTableSuffix="001",
                                uniqueTableId="G001"
)

for (t in c(Nation="Nation", Province="Province")) {
  geo_ident_tables <- newXMLNode("variable", parent=geo_ident_table) # add new child element variable to
                                                                    # //GeoSurveyDataset//tables//table
    addAttributes(geo_ident_tables, GUID=UUIDgenerate(),
                                BracketFromVal="0",
                                BracketToVal="0",
                                BracketType="None",
                                FirstInBracketSet="false",
                                notes="",
                                PrivateNotes="",
                                name=t[1],
                                label=t[1],
                                qLabel="",
                                indent="0",
                                dataType="2",
                                dataTypeLength="0",
                                formatting="0",
                                customFormatStr="",
                                FormulaFunctionBodyCSharp="",
                                suppType="0",
                                SuppField="",
                                suppFlags="",
                                aggMethod="0",
                                DocLinksAsString="",
                                AggregationStr="None")
}

# ADD SURVEY DATASET: IN THIS CASE SOCIAL EXPLORER DATASET AND ORIGINAL DATASET

survey_datasets <- xpathApply(doc, '//SurveyDatasets')  # FIND SurveyDataset

# Add Social Explorer Dataset
newXMLNode("SurveyDataset", parent=survey_datasets,
                            attrs = c(GUID=UUIDgenerate(),  # Add attributes (GUID is generated with UUIDgenerate())
                                SurveyDatasetTreeNodeExpanded="true",
                                TablesTreeNodeExpanded="true",
                                IterationsTreeNodeExpanded="false",
                                DatasetsTreeNodeExpanded="true",
                                Description="",
                                Visible="true",
                                abbreviation="SE",
                                name="Social Explorer Tables",
                                DisplayName="Social Explorer Tables"))

# Add Original Dataset
org_dataset <- newXMLNode("SurveyDataset", parent=survey_datasets,
                            attrs = c(GUID=UUIDgenerate(),  # Add attributes (GUID is generated with UUIDgenerate())
                                SurveyDatasetTreeNodeExpanded="true",
                                TablesTreeNodeExpanded="true",
                                IterationsTreeNodeExpanded="false",
                                DatasetsTreeNodeExpanded="true",
                                Description="",
                                Visible="true",
                                abbreviation="ORG",
                                name="Original Tables",
                                DisplayName="Original Tables"))

# ADD TABLE AND VARIABLES TO ORG DATASET

table_list <- list("T001", "Total Population")

tables_org_node <- newXMLNode("tables", parent=org_dataset)

table_org_node <- newXMLNode("table", parent=tables_org_node)

# Add tables
addAttributes(table_org_node, GUID=UUIDgenerate(),  # Add attributes (GUID is generated with UUIDgenerate())
                                VariablesAreExclusive="false",
                                DollarYear="0",
                                PercentBaseMin="1",
                                name=table_list[[1]],
                                displayName=table_list[[1]],
                                title=table_list[[2]],
                                titleWrapped=table_list[[2]],
                                universe="none",
                                Visible="true",
                                CategoryPriorityOrder="0",
                                ShowOnFirstPageOfCategoryListing="false",
                                DbTableSuffix=table_list[[1]],
                                uniqueTableId=table_list[[1]])

# Add variables from tables.csv

for (t in 1:nrow(variablesDF)) {
  variableNode <- newXMLNode("variable", parent=table_org_node)
    addAttributes(variableNode, GUID=UUIDgenerate(),
                                BracketFromVal="0",
                                BracketToVal="0",
                                BracketType="None",
                                FirstInBracketSet="false",
                                name=variablesDF[t,1],
                                label=variablesDF[t,2],
                                indent="0",
                                dataType="5",
                                dataTypeLength="0",
                                formatting="9",
                                suppType="0",
                                aggMethod="1",
                                AggregationStr="Add")
  newXMLNode("Visible", 'true',parent=variableNode)
}

# save xml file
saveXML(doc, file="generate_xml_R_example.xml")


