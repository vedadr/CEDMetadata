"""
This script will generate metadata file
v2.4
"""

from lxml import etree as et
from lxml.builder import ElementMaker
import uuid


def get_geotype():
    """
    Get list of geotypes in project
    :return:
    """
    e = ElementMaker()
    plural_forms = {'Nation': 'Nations', 'Province': 'Provinces'}

    # create names of relevant geos

    result = []

    for sumlev in [['SL010', 'Nation', '2', '2', '0'], ['SL040', 'Province', '4', '2', '1']]:
        result.append(e.geoType(e.Visible('true'),
                                GUID=str(uuid.uuid4()),
                                Name=sumlev[0],
                                Label=sumlev[1],
                                QLabel=sumlev[1],
                                RelevantGeoIDs='FIPS,NAME,QName',
                                PluralName=plural_forms[sumlev[1]],
                                fullCoverage='true',
                                majorGeo='true',
                                GeoAbrev='',
                                Indent=str(int(sumlev[4])),
                                Sumlev=sumlev[0].replace('SL', ''),
                                FipsCodeLength=sumlev[2],
                                FipsCodeFieldName='FIPS',
                                FipsCodePartialFieldName=sumlev[1],
                                FipsCodePartialLength=str(sumlev[3]))
                      )
    return result


def get_variables():
    """
    Get variables for original tables
    :return:
    """
    result = []
    e = ElementMaker()

    variables = [['T001_001', 'Total Population', '0'], ['T001_002', 'Male', '1'], ['T001_003', 'Female', '1']]

    for v in variables:
        result.append(e.variable(  # repeated for as many times as there are variables
            GUID=str(uuid.uuid4()),
            UVID='',
            BracketSourceVarGUID='',
            BracketFromVal='0',
            BracketToVal='0',
            BracketType='None',
            FirstInBracketSet='false',
            notes='',
            PrivateNotes='',
            name=v[0],  # meta_variable_name
            label=v[1],  # find variable description in dictionary, by text after pr. id and order
            qLabel='',
            indent=v[2],
            dataType='5',  # formatting: when 2 it is int64
            dataTypeLength='0',  # default to zero
            formatting='9',  # set to 1,234
            customFormatStr='',  # only for SE tables
            FormulaFunctionBodyCSharp='',  # only for SE tables
            suppType='0',
            SuppField='',
            suppFlags='',
            aggMethod='1',
            DocLinksAsString='',
            AggregationStr='Add'  # 'None'
        )
        )
    return result


def get_tables():
    """
    Get list of original tables from db
    :return: list with constructed tables tags
    """

    e = ElementMaker()
    result = []

    for t in [['T001', 'Total Population']]:
        result.append(
            e.tables(
                e.table(
                    e.OutputFormat(
                        e.Columns(

                        ),
                        TableTitle="",
                        TableUniverse=""
                    ),
                    *get_variables(),
                    GUID=str(uuid.uuid4()),
                    VariablesAreExclusive='false',
                    DollarYear='0',
                    PercentBaseMin='1',
                    name=t[0],
                    displayName=t[0],
                    title=t[1],
                    titleWrapped=t[1],
                    universe='none',
                    Visible='true',
                    TreeNodeCollapsed='true',
                    CategoryPriorityOrder='0',
                    ShowOnFirstPageOfCategoryListing='false',
                    DbTableSuffix=t[0],  # None when SE Tables
                    uniqueTableId=t[0]
                )
            )
        )

    return result


def get_geo_id_variables():
    """
    Get variables (geography identifiers) from "Geography Summary File"
    :return:
    """
    result = []
    e = ElementMaker()

    for i in [['SL010', 'Nation', '2', '2', '0'], ['SL040', 'Province', '4', '2', '1']]:
        result.append(e.variable(
            GUID=str(uuid.uuid4()),
            UVID='',
            BracketSourceVarGUID='',
            BracketFromVal='0',
            BracketToVal='0',
            BracketType='None',
            FirstInBracketSet='false',
            notes='',
            PrivateNotes='',
            name=i[1],
            label=i[1],
            qLabel='',
            indent='0',
            dataType='2',
            dataTypeLength='0',
            formatting='0',
            customFormatStr='',
            FormulaFunctionBodyCSharp='',
            suppType='0',
            SuppField='',
            suppFlags='',
            aggMethod='0',
            DocLinksAsString='',
            AggregationStr='None'
        )
        )
    return result


def get_geo_id_tables():
    """
    Get table "Geography Identifiers" for "Geography Summary File"
    :return:
    """
    # get_nesting_information = geo_level_info
    e = ElementMaker()
    result = [e.tables(
        e.table(
            e.OutputFormat(
                e.Columns(),
                TableTitle='',
                TableUniverse=''
            ),
            *get_geo_id_variables(),
            GUID=str(uuid.uuid4()),
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
            uniqueTableId="G001",
            source="",
            DefaultColumnCaption="",
            samplingInfo=""
        )
    )]
    return result


def create_metadata_xml():
    e = ElementMaker()

    page = e.survey(
        e.Description(et.CDATA('')
                      ),
        e.notes(et.CDATA('')
                ),
        e.PrivateNotes(et.CDATA('')
                       ),
        e.documentation(
            e.documentlinks(
            ),
            Label='Documentation'
        ),
        e.geoTypes(
             *get_geotype()
        ),
        e.GeoSurveyDataset(
            e.DataBibliographicInfo(
            ),
            e.notes(
            ),
            e.PrivateNotes(et.CDATA('')
                           ),
            e.Description(et.CDATA('')
                          ),
            e.datasets(
                #  *get_datasets(connection_string, dbname, geo_level_info,
                #  project_id, user, password, server, trusted_connection)
            ),
            e.iterations(

            ),
            *get_geo_id_tables(),
            GUID=str(uuid.uuid4()),
            SurveyDatasetTreeNodeExpanded='true',
            TablesTreeNodeExpanded='true',
            IterationsTreeNodeExpanded='false',
            DatasetsTreeNodeExpanded='true',
            Description='Geographic Summary Count',
            Visible='false',
            abbreviation='Geo',
            name='Geography Summary File',
            DisplayName='Geography Summary File'
        ),
        e.SurveyDatasets(
            e.SurveyDataset(
                e.DataBibliographicInfo(

                ),
                e.notes(

                ),
                e.PrivateNotes(
                    et.CDATA('')
                ),
                e.Description(
                    et.CDATA('')
                ),
                e.datasets(
                    #  *get_datasets(connection_string, dbname, geo_level_info,
                    #  project_id, user, password, server, trusted_connection)
                ),
                e.iterations(

                ),
                e.tables(et.Comment("Insert SE tables here !!!"),

                         ),
                GUID=str(uuid.uuid4()),
                SurveyDatasetTreeNodeExpanded='true',
                TablesTreeNodeExpanded='true',
                IterationsTreeNodeExpanded='false',
                DatasetsTreeNodeExpanded='true',
                Description='',
                Visible='false',
                abbreviation='SE',
                name='Social Explorer Tables',
                DisplayName='Social Explorer Tables'
            ),
            e.SurveyDataset(  # repeated for as many times as there are datasets
                e.DataBibliographicInfo(

                ),
                e.notes(

                ),
                e.PrivateNotes(
                    et.CDATA('')
                ),
                e.Description(
                    et.CDATA('')
                ),
                e.datasets(
                    #  *get_datasets(connection_string, dbname, geo_level_info,
                    #  project_id, user, password, server, trusted_connection)
                ),
                e.iterations(

                ),
                *get_tables(),
                GUID=str(uuid.uuid4()),
                SurveyDatasetTreeNodeExpanded='true',
                TablesTreeNodeExpanded='true',
                IterationsTreeNodeExpanded='false',
                DatasetsTreeNodeExpanded='true',
                Description='',
                Visible='true',
                abbreviation='ORG',
                name='Original Tables',
                DisplayName='Original Tables'
            )
        ),
        e.Categories(
            e.string(
                'CED'  # project_name
            )
        ),
        GUID=str(uuid.uuid4()),
        Visible='true',
        GeoTypeTreeNodeExpanded='true',
        GeoCorrespondenceTreeNodeExpanded='false',
        name='CED',  # metadata_file_name, changed from project_name when meaning of project name changed
        DisplayName='CED',
        year='2019',
        Categories=''
    )
    tree = et.ElementTree(page)
    tree.write('generate_xml_python_example.xml', pretty_print=True)
    print("Writing to: 'generate_xml_python_example.xml'")
    print("Everything finished successfully!!!")


if __name__ == '__main__':
    create_metadata_xml()
