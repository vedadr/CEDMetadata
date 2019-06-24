library(XML)
library(uuid)

# XML STRING 
empty_template.xml <- '<?xml version="1.0" encoding="utf-8"?>
<survey xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <UseOptimizedDataRetrieval>false</UseOptimizedDataRetrieval>
  <Description><![CDATA[]]></Description>
  <Notes><![CDATA[]]></Notes>
  <PrivateNotes><![CDATA[]]></PrivateNotes>
  <Documentation Label="Documentation">
    <DocumentLinks />
  </Documentation>
  <geoTypes>
  </geoTypes>
  <GeoSurveyDataset >
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
    <SurveyDataset >
      <DataBibliographicInfo />
      <notes />
      <PrivateNotes><![CDATA[]]></PrivateNotes>
      <Description><![CDATA[]]></Description>
      <datasets>
      </datasets>
      <iterations />
      <tables />
    </SurveyDataset>
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
doc = xmlTreeParse(empty_template.xml, useInternalNodes = T)     # PARSE STRING

tablesNodes <- xpathApply(doc, '//GeoSurveyDataset')             # FIND GeoSurveyDataset
xmlAttrs(tablesNodes[[1]]) <- c(GUID=UUIDgenerate(),             # Add attributes (GUID is generated with UUIDgenerate())
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

# ADD GEO TYPES


geoTypesNodes <- xpathApply(doc, '//geoTypes')             # FIND tables tag

for (g in 1:nrow(geoLevelsDF)) {
  geoNode <- newXMLNode("geoType", parent=geoTypesNodes)
  xmlAttrs(geoNode) <- c(GUID=UUIDgenerate(),
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


# ADD TABLE AND VARIABLES

tablesNodes <- xpathApply(doc, '//tables')             # FIND tables tag

for (t in 1:nrow(variablesDF)) {
  tableNode <- newXMLNode("table", parent=tablesNodes)
  xmlAttrs(tableNode) <- c(GUID=UUIDgenerate(),
                                VariablesAreExclusive="false",
                                notes="",
                                PrivateNotes="",
                                TableMapInfo="",
                                DollarYear="0",
                                PercentBaseMin="1",
                                name=variablesDF[t,1],
                                displayName=t,
                                title=variablesDF[t,2],
                                titleWrapped=variablesDF[t,2],
                                titleShort="",
                                universe="none",
                                Visible="true",
                                VisibleInMaps="true",
                                TreeNodeCollapsed="false",
                                DocSectionLinks="",
                                DataCategories="Population;",
                                ProductTags="",
                                FilterRuleName="",
                                CategoryPriorityOrder="0",
                                PaletteType="",
                                PaletteInverse="false",
                                PaletteName="",
                                ShowOnFirstPageOfCategoryListing="false",
                                DbTableSuffix=paste0("00",t),
                                uniqueTableId=paste0("CED_00",t),
                                source="",
                                DefaultColumnCaption="",
                                samplingInfo="")
  newXMLNode("Visible", 'true',parent=tableNode)
}



#print(doc)
saveXML(doc, file="CED_Auto.xml")


