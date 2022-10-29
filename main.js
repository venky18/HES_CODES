// Filter and sort function keys
const function_filter_on_csv = 'func_csv';
const function_filter_on_json = 'func_json';
const function_sort_on_json = 'func_sort';

// Tableau parameter names
const key_datasource_file_name = 'HierarchyDataSource';
const key_config_file_name = 'ConfigDataSource';
const key_columns_file_name = 'ColumnsDataSource';
const key_images_file_name = 'ImagesDataSource';
const key_network_filter_name = 'NetworkFilterId';

// Data types
const datatype_string = 'String';
const datatype_integer = 'Integer';
const datatype_float = 'Float';

// Primary Flag column key
const primary_flag_key = 'PrimaryFlag';
const primary_flag_true = ['TRUE', 'NULL']

// Config data section keys
const config_section_data = 'data';
const config_data_subsection_column_name = 'column_name';
const config_data_subsection_format_value = 'format_value';
const config_data_key_entity_id = 'entity_id';
const config_data_key_entity_name = 'entity_name';
const config_data_key_relation_type = 'relation_type';
const config_data_key_short_value = 'short_value';

const config_section_filters = 'filters';
const config_section_view = 'view';

// Config node, sidebar and searchbar section keys
const config_section_node = 'node';
const config_section_sidebar = 'sidebar';
const config_section_searchbar = 'searchbar';
const config_subsection_logo = 'logo';
const config_subsection_text = 'text';
const config_subsection_link = 'links';
const config_subsection_details = 'details';
const config_key_title_text = 'title_text';
const config_key_subtitle_text = 'subtitle_text';
const config_key_caption_text = 'caption_text';
const config_key_logo_field = 'field';
const config_key_categories = 'categories';

// Visualization element ids
const html_body_id = 'networkVisualization';
const svg_selector_ids = ['svgChart', 'svgChart2'];
const html_visualization_button_ids = ['fit_to_screen_button', 'expand_all_button', 'collapse_all_button', 'search_button'];
const html_visualization_id = 'visualizationSVGs';
const html_legend_id = 'legend';
const html_sidebar_id = 'details_sidebar';
const html_close_subhierarchy_button_id = 'close_svg_button';

const html_subhierarchy_button = 'show_sub_hierarchy_button';

// View Dimensions
const html_visualization_button_top_margin = 30;
const html_visualization_button_right_margin = 80;
const html_visualization_button_width = 40;
const html_visualization_button_height = 40;

const primary_visualization_index = 0;
const sub_hierarchy_visualization_index = 1;

// Black Red Blue Orange ForestGreen Magenta SlateGray Pink Brown Teal Green
const link_colors = ['#000000', '#FF0000', '#0000FF', '#FFA500', '#228B22', '#FF00FF', '#708090', '#FA8072', '#8B4513', '#008080', '#00FF00'];

var current_visualization_index = 0;
var max_depth = 0;
var node_index = 0;
var subhierarchy_root_node_index = 0;
var is_search_bar_open = false;
var has_network_changed = false;
var first_time_visualization_load = true;
var show_sub_hierarchy_button = false;
let primary_flag_exists = false;

var node_selected_in_primary_visualization = null;

var entity_name_column = '';
var entity_relation_type_column = '';

var selected_entity_name = '';
var selected_metric_to_display = null;

var csv_data_table = null;
var entity_data_csv = null;
var entity_data_json = null;
var visualization_data = null;

// Visualization config values
var params = {
    chart_width : 0,
    chart_height : 0,
    sidebar_width : 0,
    functions : {
        find_in_tree : {},
        show_search_results : {},
        search_entity : {},
        locate_node : {},
        find_node : {},
        fit_to_screen : {},
        collapse_all_nodes : {},
        expand_all_nodes : {},
    },
    relation_types : {},
    config_category_list: {},
	config_display_text: {},
	config_column_names: {},
	config_datatype: {},
    config_format_values: {}
};

let category_rank = {
    'high' : 3,
    'medium' : 2,
    'low' : 1
};

// Headers in column files
var column_file_headers = {
    name : 'Name',
    level : 'Level',
    key : 'Key',
    display_text : 'Text',
    datatype : 'Type',
    format : 'Format',
    category : 'Category'
};

// Headers in config file
var config_file_headers = {
    section : 'Section',
    subsection : 'Subsection',
    attribute : 'Attribute',
    value : 'Value',
    parameters : 'Parameter',
    content : 'Content'
};

// Headers in images file
var image_file_headers = {
    view : 'View',
    value : 'Value',
    path : 'Path'
};

var values_to_shorten = [];

var filter_names = [];
var filters_in_use = [];
let unregister_handler_functions = [];
let filter_parameter_values = {};
let config_data = {};
var image_paths = {};
var view_parameters = {};

/**
 * Convert the input string into title case
 * 
 * TODO: Optimize the formatting process
 * 
 * @param {*} str - String to be converted to title case
 * @returns Formatted string
 */
function to_title_case(str) {
    // Split the string by ' '
    var word_array = str.split(' ');
    
    var word_count = word_array.length;

    // Iterate over all words
	for (let i = 0; i < word_count; i+=1) {
        var word_length = word_array[i].length;

        // Iterate over all characters of corresponding word
        // This is to skip punctation symbols and convert the first alphabet to upper case
		for (let j = 0; j < word_length; j+=1) {
            if ((word_array[i].charCodeAt(j) > 96 && word_array[i].charCodeAt(j) < 123) || 
                (word_array[i].charCodeAt(j) > 64 && word_array[i].charCodeAt(j) < 91)) {
                    word_array[i] = word_array[i].substring(0, j) + word_array[i].charAt(j).toUpperCase() + word_array[i].slice(j+1).toLowerCase();
				    break;
			}
		}
    }
    
	return word_array.join(' ');
};

/**
 * Get the column index of the given column in the table
 * 
 * @param {*} table - The data table in which the column is present
 * @param {*} column - The column whose index has to be fetched
 * @returns Index of the input column. Returns -1 if column doesnot exists
 */
function get_column_index_from_table(table, column) {
    var column_data = table.columns.find(column => column.fieldName === column);

    if (column_data == undefined || column_data == null) {
        var column_name = to_title_case(column.split('_').join(' '));
        column_data = table.columns.find(column => column.fieldName === column_name);

        if (column_data == undefined || column_data == null) {
            return -1;
        }
        else {
            return column_data.index;
        }
    }
    else {
        return column_data.index;
    }
};

// Throws an error and displays an alert dialog box
function throw_error(error_message, suggestion) {
    window.alert(error_message + '\n\n' + suggestion);
    throw '';
}

function set_view_dimensions() {
    document.getElementById(html_body_id).style.display = 'block';
    document.getElementById(html_body_id).style.overflow = 'hidden';
    document.getElementById(html_body_id).style.width = window.innerWidth + 'px';
    document.getElementById(html_body_id).style.height = window.innerHeight + 'px';

    var view_parameters = config_data[config_section_view];
    
    // Set attributes mentioned in the config file
    for (let view_id in view_parameters) {
        if (view_id != html_subhierarchy_button) {
            for (let property in view_parameters[view_id]) {                
                document.getElementById(view_id).style[property] = view_parameters[view_id][property].value;
            }
        }
        else {
            show_sub_hierarchy_button = (view_parameters[view_id]['display'].value != 'none');
        }
    }

    // Set sidebar and visualization width values
    if (document.getElementById(html_sidebar_id).style.display === 'none') {
        params.chart_width = window.innerWidth - 50;
    }
    else {
        params.chart_width = (window.innerWidth - 40) * 0.83;
        params.sidebar_width = (window.innerWidth - params.chart_width - 3) < 300 ? (window.innerWidth - params.chart_width - 3) : 300;
        params.chart_width = window.innerWidth - params.sidebar_width - 3;

        document.getElementById(html_sidebar_id).style.width = params.sidebar_width + 'px';
        document.getElementById(html_sidebar_id).style.height = (window.innerHeight - 3) + 'px';
        document.getElementById(html_sidebar_id).style.left = params.chart_width + 'px';
        document.getElementById(html_sidebar_id).style.top = '0px';
        document.getElementById(html_sidebar_id).style.position = 'fixed';
    }

    // Set legend and visualization height values
    if (document.getElementById(html_legend_id).style.display === 'none') {
        params.chart_height = window.innerHeight - 10;
    }
    else {
        params.chart_height = window.innerHeight - 40;
        document.getElementById(html_legend_id).style.width = params.chart_width + 'px';
        document.getElementById(html_legend_id).style.height = '30px';
        document.getElementById(html_legend_id).style.left = '0px';
        document.getElementById(html_legend_id).style.top = (params.chart_height - 5) + 'px';
    }

    for (let i = 0; i < svg_selector_ids.length; i+=1) {
        document.getElementById(svg_selector_ids[i]).style.width = params.chart_width + 'px';
        document.getElementById(svg_selector_ids[i]).style.height = params.chart_height + 'px';
    }

    document.getElementById(html_visualization_id).style.width = params.chart_width + 'px';
    document.getElementById(html_visualization_id).style.height = params.chart_height + 'px';

    var button_top_margin = html_visualization_button_top_margin;

    // Add dimensions and margins for visualization control buttons
    for (let i = 0; i < html_visualization_button_ids.length; i+=1) {
        var button_id = html_visualization_button_ids[i];
        
        if (document.getElementById(button_id).style.display != 'none') {
            
            document.getElementById(button_id).style.width = (html_visualization_button_width + 'px');
            document.getElementById(button_id).style.height = (html_visualization_button_height + 'px');
            document.getElementById(button_id).style.left = ((params.chart_width - html_visualization_button_right_margin) + 'px');
            document.getElementById(button_id).style.top = (button_top_margin + 'px');

            button_top_margin += html_visualization_button_height;
        }
    }

    document.getElementById('user_search_box').style.height = (window.innerHeight - 2) + 'px';

    document.getElementById(html_close_subhierarchy_button_id).style.top = '10px';
    document.getElementById(html_close_subhierarchy_button_id).style.left = '10px';
}

// Read config file
function read_config_file(config_table) {
    let section_index = get_column_index_from_table(config_table, config_file_headers.section);
    let subsection_index = get_column_index_from_table(config_table, config_file_headers.subsection);
    let attribute_index = get_column_index_from_table(config_table, config_file_headers.attribute);
    let value_index = get_column_index_from_table(config_table, config_file_headers.value);
    let parameters_index = get_column_index_from_table(config_table, config_file_headers.parameters);
    let display_text_index = get_column_index_from_table(config_table, config_file_headers.content);

    let data_size = config_table.data.length;

    for (let i = 0; i < data_size; i+=1) {
        var section = config_table.data[i][section_index]['_formattedValue'];
		var subsection = config_table.data[i][subsection_index]['_formattedValue'];
		var attribute = config_table.data[i][attribute_index]['_formattedValue'];
		var value = config_table.data[i][value_index]['_formattedValue'];
		var parameters = config_table.data[i][parameters_index]['_formattedValue'];
        var content = config_table.data[i][display_text_index]['_formattedValue'];

        // Add config section as primary key
        if (!config_data.hasOwnProperty(section)) {
            config_data[section] = {};
        }

        if (subsection === '-') {
            config_data[section][attribute] = {'value' : value, 'parameters' : parameters, 'content' : content};
        }
        else {
            if (!config_data[section].hasOwnProperty(subsection)) {
                config_data[section][subsection] = {};            
            }
            
            config_data[section][subsection][attribute] = {'value' : value, 'parameters' : parameters, 'content' : content};
        }
    }

    entity_name_column = config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_name].value;
    entity_relation_type_column = config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value;

    if (config_data.hasOwnProperty(config_section_data)) {
        if (config_data[config_section_data].hasOwnProperty(config_data_subsection_format_value)) {
            if (config_data[config_section_data][config_data_subsection_format_value].hasOwnProperty(config_data_key_short_value)) {
                var column_array = config_data[config_section_data][config_data_subsection_format_value][config_data_key_short_value].value.split('|');
                
                for (let j = 0; j < column_array.length; j+=1) {
                    values_to_shorten.push(column_array[j].trim());
                }
            }
        }
    }
}

// Read columns file
function read_columns_file(columns_table) {
    // Get indices of the required columns
    let name_index = get_column_index_from_table(columns_table, column_file_headers.name);
    let level_index = get_column_index_from_table(columns_table, column_file_headers.level);
    let key_index = get_column_index_from_table(columns_table, column_file_headers.key);
    let display_text_index = get_column_index_from_table(columns_table, column_file_headers.display_text);
    let datatype_index = get_column_index_from_table(columns_table, column_file_headers.datatype);
    let format_index = get_column_index_from_table(columns_table, column_file_headers.format);
    let category_index = get_column_index_from_table(columns_table, column_file_headers.category);

    let data_size = columns_table.data.length;

    for (let i = 0; i < data_size; i+=1) {
        // Get values of the corresponding columns
        var column_name = columns_table.data[i][name_index]['_formattedValue'];
        var level = columns_table.data[i][level_index]['_formattedValue'];
        var key = columns_table.data[i][key_index]['_formattedValue'];
        var display_text = columns_table.data[i][display_text_index]['_formattedValue'];
        var datatype = columns_table.data[i][datatype_index]['_formattedValue'];
        var format = columns_table.data[i][format_index]['_formattedValue'];
        var category = columns_table.data[i][category_index]['_formattedValue'];

        if (key === primary_flag_key) {
            primary_flag_exists = true;
        }

        // Display name dictionary - JsonKey : DisplayName
        params.config_display_text[key] = display_text;

        // Format dictionary - JsonKey : String format
        params.config_format_values[key] = format;
        
        // Category dictionary - Category: [JsonKey...]
		if (category != '-') {
			if (!params.config_category_list.hasOwnProperty(category)) {
				params.config_category_list[category] = [];
			}

			params.config_category_list[category].push(key)
        }
        
        // Column Name dictionary - Depth : {JsonKey : ColumnName, ...}
		if (!params.config_column_names.hasOwnProperty(level)) {
			params.config_column_names[level] = {};

			var depth = parseInt(level);

			if (max_depth < depth) {
				max_depth = depth;
			}
		}
		params.config_column_names[level][key] = column_name;
	
		// Data type dictionary - JsonKey : Datatype
		params.config_datatype[key] = datatype;
	}

    // Remove duplicate entries in category columns list
	for (let key in params.config_category_list) {
		params.config_category_list[key] = [...new Set(params.config_category_list[key])]
	}
};

// Read images file
function read_images_file(image_table) {
    let view_index = get_column_index_from_table(image_table, image_file_headers.view);
    let value_index = get_column_index_from_table(image_table, image_file_headers.value);
    let path_index = get_column_index_from_table(image_table, image_file_headers.path);

    let data_size = image_table.data.length;

    for (let i = 0; i < data_size; i+=1) {
        var view = image_table.data[i][view_index]['_formattedValue'];
		var value = image_table.data[i][value_index]['_formattedValue'];
        var path = image_table.data[i][path_index]['_formattedValue'];
        
        // Add config section as primary key
        if (!image_paths.hasOwnProperty(view)) {
            image_paths[view] = {};
        }

        image_paths[view][value] = path;
    }
};

/**
 * This function fetches data of the selected entity from the CSV table
 * @param {string} top_level_entity : The IDN whose hierarchy has to be displayed
 */
function read_data_file(top_level_entity) {
    var requiredData = [];

    let field_index = get_column_index_from_table(csv_data_table, 
        params.config_column_names[1][entity_name_column]);

    let data_size = csv_data_table.data.length;

    // If top level entity is empty, return entire data
    if (top_level_entity === '') {
        return csv_data_table.data;
    }

	for(let i = 0; i < data_size; i+=1) {
		if (csv_data_table.data[i][field_index]['_formattedValue'] === top_level_entity) {
			requiredData.push(csv_data_table.data[i]);
		}
	}

	return requiredData;
}

// Top Called Function
$(document).ready(function () {
    tableau.extensions.initializeAsync().then(function () {
        // Get list of parameters from tableau along with the values 
        tableau.extensions.dashboardContent.dashboard.getParametersAsync().then(function (parameters) {
            parameters.forEach(function (parameter) {
                parameter.addEventListener(tableau.TableauEventType.ParameterChanged, on_parameter_change);
                
                filter_parameter_values[parameter.name] = parameter.currentValue.value;

                if (!filter_names.includes(parameter.name)) {
                    filter_names.push(parameter.name);
                }

                if (parameter.name === 'TestParam') {
                    try{
                        parameter.changeValueAsync('Value2');
                        }
                        catch(err) {
                            throw_error(err.stack)
                        }
                }
            });
        });

        fetch_filters();

        let data_source_fetch_promises = [];
        let dashboard_data_sources = {};

        const dashboard = tableau.extensions.dashboardContent.dashboard;


        // Get all worksheets placed on the dashboard
        dashboard.worksheets.forEach(function (worksheet) {
            data_source_fetch_promises.push(worksheet.getDataSourcesAsync());
        });

        // Create a list of all data sources with the meta-data
        Promise.all(data_source_fetch_promises).then(function (fetch_results) {
            fetch_results.forEach(function (worksheet_data_sources) {
                worksheet_data_sources.forEach(function (datasource) {
                    dashboard_data_sources[datasource.id] = datasource;
                });
            });
        
            access_data_sources(dashboard_data_sources);
        });
    },
    function (error) {
        throw_error('Error in loading resources', 'Check if all data sources and parameter values are correct');
    });
});

// Fetch filters from the dashboard
function fetch_filters() {
    // Removes the filter event listener
    unregister_handler_functions.forEach(function (unregister_handler_function) {
        unregister_handler_function();
    });

    let filter_fetch_promises = [];
    let dashboard_filters = [];

    const dashboard = tableau.extensions.dashboardContent.dashboard;

    dashboard.worksheets.forEach(function (worksheet) {
        filter_fetch_promises.push(worksheet.getFiltersAsync());

        unregister_handler_functions.push(worksheet.addEventListener(tableau.TableauEventType.FilterChanged, on_filter_change));
    });

    // Get Values of all filters
    Promise.all(filter_fetch_promises).then(function (fetch_results) {
        fetch_results.forEach(function (worksheet_filters) {
            worksheet_filters.forEach(function (filter) {
                dashboard_filters.push(filter);
            });
        });
        get_filter_values(dashboard_filters);
    });
}

// Callback function for filter change
function on_filter_change(filter_change_event) {
    if (filters_in_use.indexOf(filter_change_event.fieldName) != -1) {
        fetch_filters();
    }
}

// Get filter values from the dashboard
function get_filter_values(filters) {
    var filter_count = filters.length;

    for (let index = 0; index < filter_count; index+=1) {
        var filter = filters[index];

        // Add the name of the filter in filters array
        if (!filter_names.includes(filter.fieldName)) {
            filter_names.push(filter.fieldName);
        }

        // Check if the filter the the network filter
        if (filter.fieldName === filter_parameter_values[key_network_filter_name]) {
            selected_entity_name = filter.appliedValues[0].value;
            has_network_changed = true;
        }

        // If only one value is selected
        if (filter.appliedValues.length == 1) {
            filter_parameter_values[filter.fieldName] = filter.appliedValues[0].value;
        }
        else {
            // If all values are selected
            if (filter.isAllSelected == true) {
                filter_parameter_values[filter.fieldName] = 'All';
            }
            else {
                filter_parameter_values[filter.fieldName] = [];

                var filter_values_count = filter.appliedValues.length;

                for (let i = 0; i < filter_values_count; i+=1) {
                    filter_parameter_values[filter.fieldName].push(filter.appliedValues[i].value);
                }
            }
        }
    }

    if (first_time_visualization_load == false) {
        if (current_visualization_index == primary_visualization_index) {
            show_primary_hierarchy();
        }
        else {
            show_subhierarchy(subhierarchy_root_node_index);
        }
    }
}

// Callback function for parameter change
function on_parameter_change(parameter_change_event) {
	parameter_change_event.getParameterAsync().then(function (param) {
		filter_parameter_values[param.name] = param.currentValue.value;

        if (first_time_visualization_load == false) {
            if (current_visualization_index == primary_visualization_index) {
                show_primary_hierarchy();
            }
            else {
                show_subhierarchy(subhierarchy_root_node_index);
            }
        }
	});
};

// Access data sources to read the config and data files
function access_data_sources(data_sources_list) {
    // Validate whether the data sources exist in tableau
    var data_sources_names = [];

    for (let source_id in data_sources_list) {
        data_sources_names.push(data_sources_list[source_id].name);
    }

    if (!data_sources_names.includes(filter_parameter_values[key_columns_file_name])) {
        throw_error('Cannot find columns data.', 'Ensure the columns data source name is valid.');
    }
    else if (!data_sources_names.includes(filter_parameter_values[key_config_file_name])) {
        throw_error('Cannot find config data.', 'Ensure the config data source name is valid.');
    }
    else if (!data_sources_names.includes(filter_parameter_values[key_datasource_file_name])) {
        throw_error('Cannot find hierarchy data.', 'Ensure the hierarchy data source name is valid.');
    }
    else if (!data_sources_names.includes(filter_parameter_values[key_images_file_name])) {
        throw_error('Cannot find image data.', 'Ensure the image data source name is valid.');
    }
    
    for (let source_id in data_sources_list) {
        // Check if the data source is the columns config file
        if (data_sources_list[source_id].name === filter_parameter_values[key_columns_file_name]) {
            data_sources_list[source_id].getLogicalTablesAsync().then(logical_tables => {
				data_sources_list[source_id].getLogicalTableDataAsync(logical_tables[0].id).then(columns_table => {
					read_columns_file(columns_table);
				});
			});
        }

        // Check if the data source is the config file
        if (data_sources_list[source_id].name === filter_parameter_values[key_config_file_name]) {
            data_sources_list[source_id].getLogicalTablesAsync().then(logical_tables => {
                data_sources_list[source_id].getLogicalTableDataAsync(logical_tables[0].id).then(config_table => {
                    read_config_file(config_table);
                });
            });
        }

        // Check if the data source is the image file
        if (data_sources_list[source_id].name === filter_parameter_values[key_images_file_name]) {
            data_sources_list[source_id].getLogicalTablesAsync().then(logical_tables => {
                data_sources_list[source_id].getLogicalTableDataAsync(logical_tables[0].id).then(image_table => {
                    read_images_file(image_table);
                });
            });
        }
    }

    for (let source_id in data_sources_list) {
        if (data_sources_list[source_id].name === filter_parameter_values[key_datasource_file_name]) {
            data_sources_list[source_id].getLogicalTablesAsync().then(logical_tables => {
				data_sources_list[source_id].getLogicalTableDataAsync(logical_tables[0].id).then(data_table => {
                    // Save reference to the csv table and fetch data from it
                    csv_data_table = data_table;
                    show_primary_hierarchy();
                });
			});
        }
    }
};

// Creates a legend for link colors based on the relation types in the data
function create_legend_data(relation_types) {
    var relation_types = [...new Set(relation_types)];

    let type_count = relation_types.length;

    for (let i = 0; i < type_count; i+=1) {
        params.relation_types[relation_types[i]] = link_colors[i];
    }
};

/**
 * 
 * @param {*} data - The data to be filtered
 * @param {*} field - The field on which to filter the data
 * @param {*} value - The value(s) of the field to be included in the filtered data
 * @param {boolean} is_single_value - Denotes if the value is singular (true) or if it is a list of values (false)
 * @param {boolean} filter_on_all_levels - Denotes if the value should be found in any of the levels (false) or on all the levels (true)
 * @returns the filtered data
 */
function filter_csv_data(data, field, value, is_single_value=true, filter_on_all_levels=false) {
    var values_array = [];
    if (is_single_value == true) {
        if (value.toLowerCase() === 'all' || value.toLowerCase() === '' || value.toLowerCase() === 'none') {
            return data;
        }
        else {
            values_array.push(value);
        }
    }
    else if (value.length == 0) {
        return data;
    }
    else if (value.length > 1) {
        value.forEach(val => values_array.push(val.toUpperCase()));
    }

    var filtered_data = [];
    var required_columns = [];

    // Get names of required column names starting from level 2, as there will be no filters for level 1
    for (let level = 2; level <= max_depth; level+=1) {
        if(params.config_column_names[level].hasOwnProperty(field)) {
            required_columns.push(params.config_column_names[level][field]);
        }
    }

    var data_size = data.length;

    for (let i = 0; i < data_size; i+=1) {
        if (filter_on_all_levels) {
            let filter_flag = true
            for (let required_column in required_columns) {
                let field_index = get_column_index_from_table(csv_data_table, required_columns[required_column]);
                if (!values_array.includes(String(data[i][field_index]['_formattedValue'].toUpperCase()))) {
                    filter_flag = false;
                    break;
                }
            }
            if (filter_flag) {
                filtered_data.push(data[i]);
            }
        }
        else {
            for (let required_column in required_columns) {
                let field_index = get_column_index_from_table(csv_data_table, required_columns[required_column]);
                if (values_array.includes(String(data[i][field_index]['_formattedValue'].toUpperCase()))) {
                    filtered_data.push(data[i]);
                    break;
                }
            }
        }
    }

    return filtered_data;
}

// Filter json data based on the selected data
function filter_json_data(node, field, selected_data, show_only_selected_entities=false) {
    if (!node.hasOwnProperty('children')) {
        return;
    }

    let children_count = node['children'].length;

    if (show_only_selected_entities == true) {
        for (let i = 0; i < children_count; i+=1) {
            filter_json_data(node.children[i], field, selected_data, show_only_selected_entities);

            if (node.depth >= 2) {
                if (!selected_data.includes(node.children[i][field])) {
                    if (node.children[i].hasOwnProperty('children')) {
                        let sub_children_count = node.children[i].children.length;

                        for (let index=0; index < sub_children_count; index+=1) {
                            node.children.push(node.children[i].children[index]);
                        }
                    }
                    if (node.hasOwnProperty('children')) {
                        node.children.splice(i, 1);
                        i -= 1;
                        children_count -= 1;
                    }
                }
            }
        }
    }
    else {
        for (let i = 0; i < children_count; i+=1) {
            filter_json_data(node.children[i], field, selected_data, show_only_selected_entities);

            if (!selected_data.includes(node.children[i][field])) {
                if (!node.children[i].hasOwnProperty('children')) {
                    node.children.splice(i, 1);
                    i -= 1;
                    children_count -= 1;
                }
            }
        }
    }

    if (node.children.length == 0) {
        delete node.children;
    }
}

// Sort json data based on the selected metric
function sort_by_metric(node, metric=entity_name_column, is_descending_order=true) {
    if (node.hasOwnProperty('children')) {
        let child_count = node.children.length;
        if (child_count > 1) {
            for (var i = 0; i < child_count; i+=1) {
                var flag = false;
                for (var j = 0; j < child_count - 1; j+=1) {
                    var swap_elements = compare(node.children[j], node.children[j+1])
            
                    if (swap_elements > 0) {
                        [node.children[j], node.children[j+1]] = [node.children[j+1], node.children[j]]
                        flag = true;
                    }
                }

                if (flag == false) {
                     break;
                }
            }
        }

        for (let i = 0; i < node.children.length; i+=1) {
           sort_by_metric(node.children[i], metric);
        }
    }

    function compare(first, second) {
        var value1;
        var value2;
        if (metric === entity_name_column) {
            return (('' + first[entity_name_column]).localeCompare(second[entity_name_column]));
        }
        else if (params.config_datatype[metric] === 'String') {
            value1 = category_rank[first[metric].toLowerCase()];
            value2 = category_rank[second[metric].toLowerCase()];
        }
        else {
            value1 = parseInt(first[metric]);
            value2 = parseInt(second[metric]);
        }

        if (is_descending_order == true) {
            return (value2 - value1);
        }
        else {
            return (value1 - value2);
        }
    };
};

// Converts the CSV data to JSON format
function csv_to_json(csv_data, json_data, node_depth, child_parent_map = null, children_to_be_excluded = null) {
    if (node_depth > max_depth) {
        return;
    }

    var node_has_children = true;

    // Checks if the current node has any children
    if (node_depth == max_depth) {
        node_has_children = false;
    }
    else {
        var child_id_column_index =  get_column_index_from_table(csv_data_table, params.config_column_names[node_depth + 1][config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_id].value]);

        if (String(csv_data[0][child_id_column_index]['_formattedValue']).toLowerCase() === 'null' ||
            String(csv_data[0][child_id_column_index]['_formattedValue']) === '-') {
            node_has_children = false;
        }
    }

    var column_names = params.config_column_names[node_depth];

    var node = {};
    node.node_index = node_index;
    node_index += 1;
    node.depth = node_depth;

    // Adds all attributes of the entity to the node dictionary
    for (let column_name in column_names) {
        if (params.config_datatype[column_name] === datatype_string) {
            node[column_name] = csv_data[0][get_column_index_from_table(csv_data_table, column_names[column_name])]['_formattedValue'];
        }
        else if (params.config_datatype[column_name] === datatype_integer) {
            node[column_name] = parseInt(csv_data[0][get_column_index_from_table(csv_data_table, column_names[column_name])]['_value']);
        }
        else if (params.config_datatype[column_name] === datatype_float) {
            node[column_name] = parseFloat(csv_data[0][get_column_index_from_table(csv_data_table, column_names[column_name])]['_value']);
        }

        if (column_name === config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value) {
            params.relation_types[node[column_name]] = '';
        }
    }

    let node_id = node[config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_id].value];

    if (node_depth > 1) {
        if (child_parent_map) {
            if (child_parent_map.has(node_id)) {
                node['parent_nodes'] = Array.from(child_parent_map.get(node_id).values());
            }
        }
    }

    if (children_to_be_excluded) {
        if (children_to_be_excluded.includes(node_id)) {
            return;
        }
    }

    // If the node has any children
    if (node_has_children) {
        node.children = [];

        var child_id_column_index = get_column_index_from_table(csv_data_table, params.config_column_names[node_depth + 1][config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_id].value]);
        var data_size = csv_data.length;

        var child_id_list = [];

        // Gets ids of all children of the node
        for (let index = 0; index < data_size; index+=1) {
            child_id_list.push(csv_data[index][child_id_column_index]['_formattedValue']);
        }

        // Keep distinct child ids only
        var unique_child_ids = [...new Set(child_id_list)];

        // Iterate over all children
        for (let i = 0; i < unique_child_ids.length; i+=1) {
            var required_data = [];

            // Iterate over data to find data of the particular child
            for (let j = 0; j < data_size; j+=1) {
                if (csv_data[j][child_id_column_index]['_formattedValue'] === unique_child_ids[i]) {
                    required_data.push(csv_data[j]);
                }
            }

            csv_to_json(required_data, node.children, node_depth + 1, child_parent_map, children_to_be_excluded);
        }
    }

    // Adds the node to the json format
    json_data.push(node);
};

/**
 * 
 * Generates a child-parent hashmap from the 2D Array hierarchy (csv data).
 * 
 * This function returns the erroneous children, not the output. The generated output is stored in the parameter provided.
 * 
 * @param {Array} csv_data - The input hierarchy data
 * @param {Map} child_parent_map - The output child-parent hashmap
 * @returns the children with multiple primary parents
 */
function csv_to_child_parent_hashmap(csv_data, child_parent_map) {

    let children_to_be_excluded = [];

    for(let row in csv_data) {
        for (let level = 2; level <= max_depth; level++) {
            let child_id_column_index = get_column_index_from_table(csv_data_table, params.config_column_names[level][config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_id].value]);
            let child_id_relationship_type_index = get_column_index_from_table(csv_data_table, params.config_column_names[level][config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value]);
            let child_id = String(csv_data[row][child_id_column_index]['_formattedValue']);
            let child_relationship_type = String(csv_data[row][child_id_relationship_type_index]['_formattedValue']);
            if (child_id.toLowerCase() === 'null' || child_id === '-') {
                break;
            }
            
            
            // Filter out "Filled" entities
            if (!child_relationship_type.toUpperCase().includes('FILLED')) {
                
                let parent_id_column_index = get_column_index_from_table(csv_data_table, params.config_column_names[level - 1][config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_id].value]);
                let parent_id = String(csv_data[row][parent_id_column_index]['_formattedValue']);
                // Parent columns will be for the previous level, except for the primary flag and relationship_type columns
                let parent_column_names = {
                    ...params.config_column_names[level - 1],
                    [primary_flag_key]: params.config_column_names[level][primary_flag_key],
                    [config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value]: params.config_column_names[level][config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value]
                };

                if (child_parent_map.has(child_id)) {

                    // Get existing parents in hashmap
                    let parents = child_parent_map.get(child_id);
                        
                    if (!parents.has(parent_id)) {
                        
                        // Adding all attributes to the parent object
                        let parent_node = {};
                        for (let column_name in parent_column_names) {
                            if (params.config_datatype[column_name] === datatype_string) {
                                parent_node[column_name] = csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_formattedValue'];
                            }
                            else if (params.config_datatype[column_name] === datatype_integer) {
                                parent_node[column_name] = parseInt(csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_value']);
                            }
                            else if (params.config_datatype[column_name] === datatype_float) {
                                parent_node[column_name] = parseFloat(csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_value']);
                            }
                        }
                        
                        // Multiple primary parent check
                        if (parent_node[primary_flag_key].toUpperCase() === 'TRUE') {
                            for (parent of parents.values()) {
                                if (parent[primary_flag_key].toUpperCase() === 'TRUE') {
                                    children_to_be_excluded.push(child_id);
                                }
                            }
                        }
                        
                        parents.set(parent_id, parent_node);
                    }
                }
                else {
                    let parents = new Map();
                    
                    // Adding all attributes to the parent object
                    let parent_node = {};
                    for (let column_name in parent_column_names) {
                        if (params.config_datatype[column_name] === datatype_string) {
                            parent_node[column_name] = csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_formattedValue'];
                        }
                        else if (params.config_datatype[column_name] === datatype_integer) {
                            parent_node[column_name] = parseInt(csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_value']);
                        }
                        else if (params.config_datatype[column_name] === datatype_float) {
                            parent_node[column_name] = parseFloat(csv_data[row][get_column_index_from_table(csv_data_table, parent_column_names[column_name])]['_value']);
                        }
                    }
                    parents.set(parent_id, parent_node);
                    child_parent_map.set(child_id, parents);
                }
            }
        }
    }

    // Unique children to be excluded
    children_to_be_excluded = [...new Set(children_to_be_excluded)];
    return children_to_be_excluded;
}

/**
 * This function rolls up the selected metric values from the leaf to the root of the hierarchy
 * 
 * ? Should the visualizer roll up data?
 * 
 * @param {*} json_data - Hierarchy data in JSON format
 * @param {*} metrics - List of metrics to be rolled up
 */
function rollup_metrics(json_data, metrics) {
    // If the node is leaf node, return from the current call
    if (!json_data.hasOwnProperty('children')) {
        return;
    }

    // Recursive call for each of the children
    for (let i=0; i<json_data.children.length; i+=1) {
        rollup_metrics(json_data.children[i], metrics);
    }

    // For each parent node, add up the metrics of all children and the parent's metric
    for (let metric_index=0; metric_index < metrics.length; metric_index += 1) {
        total_metric = 0;

        for (let index=0; index < json_data.children.length; index += 1) {
            total_metric += json_data.children[index][metrics[metric_index]];
        }

        json_data[metrics[metric_index]] += total_metric;
    }
}

function process_data(input_data, is_csv_data=true, child_parent_map, children_to_be_excluded=null) {
    var required_data = input_data;

    filters_in_use = [filter_parameter_values[key_network_filter_name]];
    
    var json = {};

    if (is_csv_data == true) {
        if (config_data.hasOwnProperty(config_section_filters)) {
            if (config_data[config_section_filters].hasOwnProperty(function_filter_on_csv)) {
                var csv_filters = config_data[config_section_filters][function_filter_on_csv];

                for (let filter in csv_filters) {
                    if (!filter_names.includes(filter)) {
                        throw_error('Cannot find filter ' + filter, 'Please check the filter name in config file');
                    }

                    filters_in_use.push(filter);

                    // The last parameter indicates whether the value is a list or a single value
                    required_data = filter_csv_data(required_data, csv_filters[filter].value, filter_parameter_values[filter], typeof(filter_parameter_values[filter]) != 'object');
                }
            }
        }

        if (primary_flag_exists) {
            required_data = filter_csv_data(required_data, primary_flag_key, primary_flag_true, typeof(primary_flag_true) != 'object', true);
        }

        // If the data array is empty, clear the visualization
        if (required_data.length == 0) {
            d3.selectAll('svg').remove();
            throw_error('Empty data on filtering', 'Check whether the value exists in the data');
        }

        var json_format = [];
        node_id = 1;
        // Generate json from csv format
        csv_to_json(required_data, json_format, 1, child_parent_map, children_to_be_excluded);

        entity_data_json = json_format[0];

        //rollup_metrics(entity_data_json, ['NetIncome']);
        json = JSON.parse(JSON.stringify(entity_data_json));

        console.log(json);
    }
    else {
        json = input_data;
    }

    sort_by_metric(json);

    if (config_data.hasOwnProperty(config_section_filters)) {
        if (config_data[config_section_filters].hasOwnProperty(function_filter_on_json)) {
            var json_filters = config_data[config_section_filters][function_filter_on_json];

            for (let filter in json_filters) {
                if (!filter_names.includes(filter)) {
                    throw_error('Cannot find filter ' + filter, 'Please check the filter name in config file');
                }

                filters_in_use.push(filter);

                var filter_values = [];

                if (typeof(filter_parameter_values[filter]) === 'object') {
					filter_values = filter_parameter_values[filter].map(v => v);
				}
				else {
					filter_values.push(filter_parameter_values[filter]);
                }
                
                if (filter_values[0].toLowerCase() != 'all' && filter_values[0].toLowerCase() != 'none' && filter_values[0] != '') {
					filter_json_data(json, json_filters[filter].value, filter_values, json_filters[filter].parameters.toLowerCase() === 'true');
				}
            }
        }
    }

    if (config_data.hasOwnProperty(config_section_filters)) {
        if (config_data[config_section_filters].hasOwnProperty(function_sort_on_json)) {
            var sort_filters = config_data[config_section_filters][function_sort_on_json];

            for (let filter in sort_filters) {
                if (!filter_names.includes(filter)) {
                    throw_error('Cannot find filter ' + filter, 'Please check the filter name in config file');
                }

                filters_in_use.push(filter);

                var sort_column = sort_filters[filter].value;

                if (sort_filters[filter].value.includes('{')) {
                    sort_column = filter_parameter_values[filter];
                }

                if (filter_parameter_values[filter].toLowerCase() != 'none') {
                    selected_metric_to_display = filter_parameter_values[filter];
                    sort_by_metric(json, sort_column);
                }
                else {
                    selected_metric_to_display = null;
                }
            }
        }
    }

    return json;
};

function show_primary_hierarchy() {
    params.relation_types = {};

    console.log(csv_data_table.data);
    let children_to_be_excluded = null;
    let child_parent_map = null;
    if (primary_flag_exists) {
        child_parent_map = new Map();
        children_to_be_excluded = csv_to_child_parent_hashmap(csv_data_table.data, child_parent_map);
        console.log(child_parent_map);
    }

    entity_data_csv = read_data_file(selected_entity_name); //filter for IDN

    current_visualization_index = primary_visualization_index;
    try {
        visualization_data = process_data(entity_data_csv, true, child_parent_map, children_to_be_excluded);
    }
    catch (err) {
        window.alert(err.stack);
    }
    set_view_dimensions();

    d3.selectAll("svg").remove();
    create_legend_data(Object.keys(params.relation_types));

    if (first_time_visualization_load == true) {
        // Called when the window is resized
        window.onresize = function(){
            set_view_dimensions();

            current_visualization_index = primary_visualization_index;
            d3.selectAll("svg").remove();
            draw_organization_chart(params, visualization_data, current_visualization_index);
        }
    }

    first_time_visualization_load = false;

    try {
        draw_organization_chart(params, visualization_data, current_visualization_index);
    }
    catch(err) {
        throw_error('Cannot render visualization', err.stack)
    }
};

function show_subhierarchy(selected_node_id) {
    subhierarchy_root_node_index = selected_node_id;
    document.getElementById(svg_selector_ids[primary_visualization_index]).style.display = 'none';
    document.getElementById(svg_selector_ids[sub_hierarchy_visualization_index]).style.display = 'block';
	document.getElementById(html_close_subhierarchy_button_id).style.display = 'block';

	current_visualization_index = sub_hierarchy_visualization_index;

	document.getElementById(svg_selector_ids[sub_hierarchy_visualization_index]).innerHTML = '';

	var data = get_subhierarchy_data(JSON.parse(JSON.stringify(entity_data_json)), subhierarchy_root_node_index);
	
	data = process_data(data, false);
	
	draw_organization_chart(params, data, current_visualization_index);

    // Function to get the sub-hierarchy data
	function get_subhierarchy_data(data, selected_node_id) {
		var subhierarchy_data = data;
		while (true) {
			if (subhierarchy_data.node_index == selected_node_id) {
				break;
			}
			else {
				for (let i = subhierarchy_data.children.length - 1; i >= 0; i-=1) {
					if (selected_node_id >= subhierarchy_data.children[i].node_index) {
						subhierarchy_data = subhierarchy_data.children[i];
						break;
					}
				}
			}
		}

		return subhierarchy_data;
    }
};

function close_sub_hierarchy() {
    document.getElementById(svg_selector_ids[primary_visualization_index]).style.display = 'block';
    document.getElementById(svg_selector_ids[sub_hierarchy_visualization_index]).style.display = 'none';
	document.getElementById(html_close_subhierarchy_button_id).style.display = 'none';

	current_visualization_index = primary_visualization_index;

    document.getElementById(svg_selector_ids[sub_hierarchy_visualization_index]).innerHTML = '';
    
    // Retain the selected node in the primary visualization
    var elem = document.getElementById(get_element_id('node' + node_selected_in_primary_visualization));
	if (elem != null) {
		var evt = document.createEvent("MouseEvents");
		evt.initMouseEvent(
		"click", // Type
		true, // Can bubble
		true, // Cancelable
		window, // View
		0, // Detail
		0, // ScreenX
		0, // ScreenY
		0, // ClientX
		0, // ClientY
		false, // Ctrl key
		false, // Alt key
		false, // Shift key
		false, // Meta key
		0, // Button
		null); // RelatedTarget

        elem.dispatchEvent(evt);
    }
};

function expand_all_function() {
	params.functions.expand_all_nodes[current_visualization_index]()
}

function collapse_all_function() {
	params.functions.collapse_all_nodes[current_visualization_index]()
}

function fit_to_screen_function() {
	params.functions.fit_to_screen[current_visualization_index]()
}

// Returns the reference to the element with specified ID
function get(selector) {
    return document.querySelector(selector);
};

// Returns the reference to the list of elements with specified ID
function get_all(selector) {
    return document.querySelectorAll(selector);
};

// Sets the HTML content of the specified element to the given value
function set(selector, value) {
    var elements = get_all(selector);
    elements.forEach(function (element) {
        element.innerHTML = value;
        element.value = value;
    })
};

// Removes all children of the specified element
function clear(selector) {
    set(selector, '');
};

// Clears the list of searched entities
function clear_search_results() {
    set('.result-list', '<div class="buffer"></div>');
    set('.user-search-box .result-header', "RESULT");
};

// Toggles the visibility of the search bar
function toggle_search_bar() {
    if (!is_search_bar_open) {
        d3.selectAll('.user-search-box')
            .transition()
            .duration(250)
            .style('width', params.sidebar_width + 'px');	
    }
    else {
        d3.selectAll('.user-search-box')
            .transition()
            .duration(250)
            .style('width', '0px')
            .each("end", function () {
                clear_search_results();
                clear('.search-input');
            });
    }

    is_search_bar_open = !is_search_bar_open;
};

// Closes the search bar
function close_search_bar() {
    d3.selectAll('.user-search-box')
        .transition()
        .duration(250)
        .style('width', '0px')
        .each("end", function () {
            clear_search_results();
            clear('.search-input');
        });

        is_search_bar_open = false;
};