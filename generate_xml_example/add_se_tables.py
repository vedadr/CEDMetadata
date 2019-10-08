from lxml import etree as et
import lxml.etree
import uuid

# Please change the information here when running the script for each new project/survey

# this is the xml with social explorer tables
old_xml_file_name = r'D:/Projects/CEDMetadata/CED2000 - Copy.xml'
old_org_abbreviation = 'CED2000'

# This is the CED Tables abbreviation
CED_tables_abbreviation = 'SE'

# this is the new xml where you want to copy CED Tables with formulas from previous year
new_xml_file_name = r'D:/Projects/CEDMetadata/CED2001 - Copy.xml'
new_org_abbreviation = 'CED2001'


def get_previous_xml_formulas():
    """
    Get formulas other than 'ADD' aggregation method
    Here is added median, weighted average, Rate, Division of SUms
    If you want to add additional agg.methods such as Percent and similar, you'll need to add it in the formula loop
    :return: formula_dict Dict with formulas for each se tables other than 'ADD'
    """

    old_formula_dict = {}

    for var in tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='" +
                          CED_tables_abbreviation+"']/tables/table/variable"):
        reverse_dict = {val: key for key, val in all_guid.items()}
        split_vars = var.attrib['AggregationStr'].split('|')
        if var.attrib['AggregationStr'].startswith('Median') and var.attrib['AggregationStr'].split('|')[2] != '':
            old_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             split_vars[1]+'|' +
                                                             reverse_dict[split_vars[2]]+'|' +
                                                             split_vars[3]+'|' +
                                                             split_vars[4]+'|' +
                                                             split_vars[5]])
        elif var.attrib['AggregationStr'].startswith('DivisionOfSums') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            old_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             reverse_dict[split_vars[1]]+'|' +
                                                             reverse_dict[split_vars[2]]])
        elif var.attrib['AggregationStr'].startswith('WeightedAvg') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            old_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             reverse_dict[split_vars[1]]])

        elif var.attrib['AggregationStr'].startswith('Rate') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            old_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             reverse_dict[split_vars[1]]+'|' +
                                                             reverse_dict[split_vars[2]]+'|' +
                                                             split_vars[3]])

    return old_formula_dict


def get_se_vars():
    """
    Access to the variables nodes in the old xml file, and change GUIDs and tables abbreviation
    :return: tables with the new variables (new GUIDS and replaced abbreviations
    """

    previous_xml_tables = tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='" +
                                     CED_tables_abbreviation+"']/tables")[0]

    new_table_guid_redistribute = {}

    for previous_table in previous_xml_tables:
        previous_table.attrib['GUID'] = str(uuid.uuid4())
        new_table_guid_redistribute[previous_table.attrib['name']] = previous_table.attrib['GUID']

    new_variable_guid_redistribute = {}

    for var in tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='" +
                          CED_tables_abbreviation+"']/tables/table/variable"):
        var.attrib['GUID'] = str(uuid.uuid4())
        new_variable_guid_redistribute[var.attrib['name']] = var.attrib['GUID']
        if old_org_abbreviation in var.attrib['FormulaFunctionBodyCSharp']:
            var.attrib['FormulaFunctionBodyCSharp'] = var.attrib['FormulaFunctionBodyCSharp'].replace(
                old_org_abbreviation, new_org_abbreviation)

    all_new_table_guid_redistribute = {**new_table_guid_redistribute, **new_variable_guid_redistribute}

    return previous_xml_tables, all_new_table_guid_redistribute


def get_new_formulas():
    for var in tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='" +
                          CED_tables_abbreviation+"']/tables/table/variable"):
        if var.attrib['AggregationStr'] != 'Add' and var.attrib['AggregationStr'] != 'None':
            if var.attrib['name'] in formula_dict:
                var.attrib['AggregationStr'] = formula_dict[var.attrib['name']][0]
        if old_org_abbreviation in var.attrib['AggregationStr']:
            var.attrib['AggregationStr'] = var.attrib['AggregationStr'].replace(
                old_org_abbreviation, new_org_abbreviation)

    var_name_list = []
    for var_name in tree.xpath("//SurveyDatasets/SurveyDataset/tables/table/variable"):
        var_name_list.append(var_name.attrib['name'])

    new_formula_dict = {}
    for var in tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='"
                          + CED_tables_abbreviation+"']/tables/table/variable"):
        split_vars = var.attrib['AggregationStr'].split('|')
        if var.attrib['AggregationStr'].startswith('Median') and var.attrib['AggregationStr'].split('|')[2] != '':
            new_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             split_vars[1]+'|' +
                                                             all_new_guid_se_org[split_vars[2]]+'|' +
                                                             split_vars[3]+'|' +
                                                             split_vars[4]+'|' +
                                                             split_vars[5]])
        elif var.attrib['AggregationStr'].startswith('DivisionOfSums') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            new_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             all_new_guid_se_org[split_vars[1]]+'|' +
                                                             all_new_guid_se_org[split_vars[2]]])
        elif var.attrib['AggregationStr'].startswith('WeightedAvg') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            new_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             reverse_dict[split_vars[1]]])

        elif var.attrib['AggregationStr'].startswith('Rate') and\
                var.attrib['AggregationStr'].split('|')[1] != '':
            new_formula_dict.setdefault(var.attrib['name'], [split_vars[0]+'|' +
                                                             all_new_guid_se_org[split_vars[1]]+'|' +
                                                             all_new_guid_se_org[split_vars[2]]+'|' +
                                                             split_vars[3]])

    for var in tree.xpath("//SurveyDatasets/SurveyDataset[@abbreviation='" +
                          CED_tables_abbreviation+"']/tables/table/variable"):
        if var.attrib['AggregationStr'] != 'Add' and var.attrib['AggregationStr'] != 'None':
            if var.attrib['name'] in new_formula_dict:
                var.attrib['AggregationStr'] = new_formula_dict[var.attrib['name']][0]

    return tables


def get_new_xml():
    """
    Access to the tables node in the new xml file
    :return: tables_new
    """

    tables_new = new_tree.xpath("SurveyDatasets/SurveyDataset[@abbreviation='"+CED_tables_abbreviation+"']/tables")

    return tables_new


def copy_to_new_xml():
    """
    append SE tables and variables from previous xml file to the new xml file
    :return:
    """
    se_vars_by_table = get_new_formulas()
    new_xml_se_vars_by_table = get_new_xml()
    new_xml_se_vars_by_table[0].extend(se_vars_by_table)

    new_tree.write('D:/Projects/CEDMetadata/CED2001_copy.xml')


if __name__ == "__main__":

    parser_old = lxml.etree.XMLParser(strip_cdata=False)
    tree = lxml.etree.parse(old_xml_file_name, parser_old)
    input_old_xml = tree.getroot()

    parser_new = lxml.etree.XMLParser(strip_cdata=False)
    new_tree = lxml.etree.parse(new_xml_file_name, parser_new)
    root_new = new_tree.getroot()

    all_new_variables = new_tree.xpath("//SurveyDatasets/SurveyDataset/tables/table/variable")
    all_new_tables = new_tree.xpath("//SurveyDatasets/SurveyDataset/tables/table")

    all_old_variables = tree.xpath("//SurveyDatasets/SurveyDataset/tables/table/variable")
    all_old_tables = tree.xpath("//SurveyDatasets/SurveyDataset/tables/table")

    variable_guid = {}
    for variable in all_old_variables:
        variable_guid[variable.attrib['name']] = variable.attrib['GUID']

    table_guid = {}
    for table in all_old_tables:
        table_guid[table.attrib['name']] = table.attrib['GUID']

    all_guid = {**variable_guid, **table_guid}

    new_variable_guid = {}
    for variable in all_new_variables:
        new_variable_guid[variable.attrib['name']] = variable.attrib['GUID']

    new_table_guid = {}
    for table in all_new_tables:
        new_table_guid[table.attrib['name']] = table.attrib['GUID']

    all_new_guid = {**new_variable_guid, **new_table_guid}

    # in order for some function to debug, you'll need to call it here
    tables, all_new_table_guid = get_se_vars()
    all_new_guid_se_org = {**all_new_table_guid, **all_new_guid}
    formula_dict = get_previous_xml_formulas()
    copy_to_new_xml()
