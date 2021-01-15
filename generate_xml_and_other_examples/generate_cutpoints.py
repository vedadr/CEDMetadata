from pathlib import Path
import jenkspy
import pandas as pd
from loguru import logger
from lxml.builder import ElementMaker
from lxml import etree as et


def calculate_brackets(data, number_of_classes):
    breaks = {}
    for variable_name, values in data.iteritems():
        try:
            breaks[variable_name] = jenkspy.jenks_breaks(values, nb_class=number_of_classes)
        except ValueError as e:
            logger.warning(f'Cannot create a cupoint file for variable {variable_name}, error: {e}')
            continue
        except TypeError as e:
            logger.error(f'Cutpoints cannot be created for variable {variable_name}, reason: {e}')
    return breaks


def generate_xml_with_cutpoints(brackets, output_path, project_id):
    """
    Naming convention project name + name in title case e.g. COVID19Cases, COVID19Deaths
    Example output xml:

    <CategoryFilters name="COVID19Cases" valueFormat="Number">
        <FilterSet>
            <Filter from="" to="271" />
            <Filter from="271" to="479" />
            <Filter from="479" to="838" />
            <Filter from="838" to="1066" />
            <Filter from="1066" to="" />
 	    </FilterSet>
    </CategoryFilters>

    :param brackets:
    :param output_path:
    :return:
    """

    for file_name, values in brackets.items():
        xml_structure = ElementMaker()

        filter_nodes = []
        for i, value in enumerate(values):
            e = ElementMaker()

            start = str(int(value)) if value != 0 else ''
            end = str(int(values[i + 1])) if i + 1 < len(values) else ''

            filter_nodes.append(e.Filter(**{'from': start, 'to': end}))

        page = xml_structure.CategoryFilters(
            e.FilterSet(
                *filter_nodes
            ),
            name='cutpoints_' + project_id +'_'+ file_name.title(),
            valueFormat="Number"
        )

        tree = et.ElementTree(page)
        logger.info(f'Writing cutpoints for {file_name}')
        tree.write(output_path + '/' + project_id + file_name + '.xml', pretty_print=True)


def generate_cutpoints_for_dataset():
    # Just adjust these according to your needs
    path_to_preprocessed_files = Path(r'C:\projects\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports')
    output_location = '.'
    project_id = 'ELEC2019'
    number_of_classes = 11
    #########################################################

    for file in path_to_preprocessed_files.glob('*.csv'):
        file_content = pd.read_csv(str(file)).dropna()

        brackets = calculate_brackets(file_content, number_of_classes)
        generate_xml_with_cutpoints(brackets, output_location, project_id)


if __name__ == '__main__':
    generate_cutpoints_for_dataset()
