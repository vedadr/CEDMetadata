"""
This script will generate metadata file
v2.4
"""

from lxml import etree as et
from lxml.builder import ElementMaker
import uuid
import pymssql
import csv
import os
import yaml
import sys
import collections  # used for dictionary sorting
import datetime  # used for validation
import optparse
from sys import argv


def get_geotype():  # number of geo types
    """
    Get list of geotypes in project
    :param geo_level_info: List from config file
    :return:
    """
    e = ElementMaker()
    plural_forms = {'Nation': 'Nations', 'Province': 'Provinces'}

    # create names of relevant geos

    result = []

    for sumlev in [['Nation', 'Nation', '2', '2', '0'],['Province', 'Province', '4', '2', '1']]:
        result.append(e.geoType(e.Visible('true'),
                                GUID=str(uuid.uuid4()),
                                Name=sumlev[0],
                                Label=sumlev[1],
                                QLabel=sumlev[1],
                                RelevantGeoIDs='FIPS,NAME,QName,',
                                PluralName=plural_forms[sumlev[1]],
                                fullCoverage='true',
                                majorGeo='true',
                                # GeoAbrev = sumlev[0],#'us, nation', COMMENTED BECAUSE IN ACS 2011 EXAMPLE IT WAS
                                # MISSING!?
                                Indent=str(int(sumlev[4])),
                                Sumlev=sumlev[0].replace('SL', ''),
                                FipsCodeLength=sumlev[2],
                                FipsCodeFieldName='FIPS',
                                FipsCodePartialFieldName=sumlev[0],
                                FipsCodePartialLength=str(sumlev[3]))
                      )
    return result


def get_variables():
    """
    Get variables for original tables
    :param variables: List of variables
    :param variable_description: List of variable descriptions
    :param meta_table_name: Table name for metadata file, extracted from data file name
    :return:
    """
    result = []
    e = ElementMaker()

    variables = [['Total Population', '0'], ['Male', '1'], ['Female', '1']]

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
            label=v[0],  # find variable description in dictionary, by text after pr. id and
            #  order
            qLabel='',
            indent=v[1],
            dataType='2',
            dataTypeLength='0',  # default to zero
            formatting='2',
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
    :param server: Server name
    :param dbname: Database name
    :param variable_description: List to decode variable names into descriptions (from variable_descriptions file)
    :param user: Username for server
    :param password: Password for server
    :param project_year: Project year
    :return: list with constructed tables tags
    """

    E = ElementMaker()
    result = []

    for t in ['Total Population']:
        result.append(
            E.tables(
                E.table(
                    E.OutputFormat(
                        E.Columns(

                        ),
                        TableTitle="",
                        TableUniverse=""
                    ),
                    *get_variables()
                    ,
                    GUID=str(uuid.uuid4()),
                    VariablesAreExclusive='false',
                    DollarYear='0',
                    PercentBaseMin='1',
                    name=t,
                    displayName=t,
                    title=t,
                    titleWrapped=t,
                    universe='none',
                    Visible='true',
                    TreeNodeCollapsed='true',
                    CategoryPriorityOrder='0',
                    ShowOnFirstPageOfCategoryListing='false',
                    DbTableSuffix=t,
                    uniqueTableId=t
                )
            )
        )

    return result


def get_geo_id_variables():
    """
    Get variables (geography identifiers) from "Geography Summary File"
    :param geoLevelInfo: List from config file
    :return:
    """
    result = []
    E = ElementMaker()
    for i in [['Nation', 'Nation', '2'],['Province', 'Province', '2']]:
        result.append(E.variable(
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
            dataType=i[2],
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
    :param geo_level_info: List from config file
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
                #  *get_datasets(connection_string, dbname, geo_level_info, project_id, user, password, server, trusted_connection)
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
                    #  *get_datasets(connection_string, dbname, geo_level_info, project_id, user, password, server, trusted_connection)
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
                    #  *get_datasets(connection_string, dbname, geo_level_info, project_id, user, password, server, trusted_connection)
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
                'CED' #  project_name
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
    tree.write('generate_xml_python_example.xml')
    print("Writing to: 'generate_xml_python_example.xml'")
    print("Everything finished successfully!!!")


if __name__ == '__main__':
    create_metadata_xml()
