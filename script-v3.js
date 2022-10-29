const template_delimiter = '|';

const nodes_per_page = 3;
const decimal_characters_for_metric = 3;

const transition_duration = 600;

const EXPAND_SYMBOL = '+';
const COLLAPSE_SYMBOL = '-';

const min_max_zoom_proportions = [0.5, 2];
const link_line_size = 150;
const child_count_icon = '\uf0f8';

const node_stroke = 'teal';
const selected_node_background = '#ffeeff';
const dummy_node_background = '#dedede';
const true_node_background = '#ffffff';

const icon_path_next = './static/svg/next_2.svg';
const icon_path_previous = './static/svg/prev_2.svg';

var dimens = {};

// Converts the template from the config file into text with data
function get_text_from_template(node, template, parameters) {
    // If the value is '-', return null to hide the view
    if (template === '-' || template.toLowerCase() === 'null') {
        return null;
    }
    // If the value is not data dependent, return the constant value
    if (parameters === '-' || parameters.toLowerCase() === 'null') {
        return template;
    }

    var fields = parameters.split(template_delimiter);
    var text = template;

	var flag = false;

    for (let i = 0; i < fields.length; i+=1) {
		var field = fields[i].trim();
        text = text.replace('###' + i, node[field]);
	
		flag = flag | (node[field] != '-');
	}

	if (flag == false) {
		return '';
	}
    return text;
};

// Converts the number into formats with K, M, B and T
function number_to_text(number) {
	var suffixes = ['', ' K', ' M', ' B', ' T'];

	if (number > 1000) {
		var integer = Math.floor(number);
		var int_string = String(integer);
		var power = Math.floor((int_string.length - 1) / 3);
		var integral_digits = (int_string.length % 3);

		if (integral_digits == 0) { integral_digits = 3; }

		var short_format = '';
		
		for (var i=0;i<integral_digits;i+=1) {
			short_format = short_format + int_string[i]
		}

		short_format = short_format + '.'

		short_format = short_format + int_string.substr(integral_digits, decimal_characters_for_metric);

		return short_format + suffixes[power];
	}

	return Math.floor(number);
}

// Creates seperate IDs for elements in different SVGs
function get_element_id(element_id, include_hash=false) {
	var id = (element_id + '_' + current_visualization_index);
	if (include_hash) {
		id = '#' + id;
	}

	return id;
};

function draw_organization_chart(params, data, svg_index = 0) {
	document.getElementById(html_sidebar_id).innerHTML = '';

	var current_svg_id = svg_selector_ids[svg_index];

	params.functions.find_in_tree[svg_index] = findInTree;
	params.functions.show_search_results[svg_index] = show_search_results;
	params.functions.locate_node[svg_index] = locate_node;
	params.functions.find_node = find_node;
	params.functions.fit_to_screen[svg_index] = fit_to_screen;
	params.functions.collapse_all_nodes[svg_index] = collapse_all_nodes;
	params.functions.expand_all_nodes[svg_index] = expand_all_nodes;
	params.functions.search_entity[svg_index] = searchbar_handler;

	searchbar_listener();

	var selected_node_id = '';

	var hierarchy_data = data;

	var temp_node_index = 0;
	
	// Element dimensions
	dimens.chart_width = params.chart_width;
	dimens.chart_height = params.chart_height - 3;
	dimens.node_width = ((dimens.chart_width / 4) < 270)? (dimens.chart_width / 4) : 270;
	dimens.node_height = (80 * dimens.node_width) / 270;    
	dimens.node_padding = dimens.node_width / 30;
	
	dimens.node_icon_width = 0;
	dimens.node_icon_height = 0;

	if (config_data[config_section_node][config_subsection_logo][config_key_logo_field].value != '-') {
		dimens.node_icon_width = parseInt(dimens.node_height * 90 / 190);
		dimens.node_icon_height = parseInt(dimens.node_height - 2 * dimens.node_padding - 5);
	}


	// Element sizes
	dimens.collapse_circle_radius = dimens.node_width / 30;
	dimens.node_stroke_width = '1px';
	dimens.selected_node_stroke_width = '3px';

	// Font sizes
	dimens.legend_text_font_size = parseInt((14 * dimens.node_width) / 290);
	dimens.collapible_font_size = 15;
	dimens.node_title_font_size = parseInt(11 * dimens.node_width / 270);
	dimens.node_subtitle_font_size = parseInt(11 * dimens.node_width / 270);
	dimens.node_caption_font_size = parseInt(11 * dimens.node_width / 270);
	dimens.node_child_count_font_size = parseInt(11 * dimens.node_width / 270);
	dimens.sidebar_title_font_size = ((14 * dimens.node_width) / 270);
	dimens.sidebar_subtitle_font_size = ((12 * dimens.node_width) / 270);
	dimens.sidebar_caption_font_size = ((12 * dimens.node_width) / 270);
	dimens.sidebar_content_font_size = Math.ceil((14 * params.sidebar_width) / 300);

	// Element paddings and margins
	dimens.root_node_left_margin = (dimens.chart_width - dimens.node_width) / 2;
	dimens.root_node_top_margin = 20;
	dimens.node_title_top_margin = parseInt(dimens.node_padding + 7);
	dimens.node_subtitle_top_margin = parseInt(dimens.node_height / 1.75 - 7);
	dimens.node_caption_top_margin = parseInt(dimens.node_subtitle_top_margin + dimens.node_subtitle_font_size + 10);
	dimens.node_text_left_margin = parseInt(dimens.node_icon_width + 2 * dimens.node_padding);
	dimens.node_child_count_left_margin = dimens.node_width - 40;
	dimens.node_child_count_top_margin = dimens.node_height - 7;
	dimens.multi_parent_indicator_source_x = -30
	dimens.multi_parent_indicator_source_y = 20
	dimens.multi_parent_indicator_dest_x = -10
	dimens.multi_parent_indicator_dest_y = 39
	
    // Render the legend
    if (document.getElementById(html_legend_id).style.display != 'none') {
        document.getElementById(html_legend_id).innerHTML = '';

        var relation_type_list = Object.keys(params.relation_types);

        for (let i = 0; i < relation_type_list.length; i+=1) {
            var style = `style="margin-left: 5px; font-size: ${dimens.legend_text_font_size}px;"`;
            document.getElementById(html_legend_id).innerHTML += `<li ${style}><span style="background-color: ${params.relation_types[relation_type_list[i]]}; height:5px; width: 15px; margin-top: 8px; margin-left: 5px;"></span> ${to_title_case(relation_type_list[i])}</li>`;
        }
	}
	
	var selectedNode = null;
	var draggingNode = null;

	var tree = d3.layout.tree()
				.nodeSize([dimens.node_width + 40, dimens.node_height])
				.separation(function(a, b) {
					return a.parent == b.parent ? 1 : 1.15;
				});

	// Draws links from source node to destination node
	function draw_link(source, target) {
		const x = source.x + dimens.node_width / 2;
		const y = source.y + dimens.node_height / 2;
		const ex = target.x + dimens.node_width / 2;
		const ey = target.y + dimens.node_height / 2;

		let xrvs = ex - x < 0 ? -1 : 1;
		let yrvs = ey - y < 0 ? -1 : 1;

		let rdef = 35;
		let r = Math.abs(ex - x) / 2 < rdef ? Math.abs(ex - x) / 2 : rdef;

		r = Math.abs(ey - y) / 2 < r ? Math.abs(ey - y) / 2 : r;

		let h = Math.abs(ey - y) / 2 - r;
		let w = Math.abs(ex - x) - r * 2;
		//w=0;
		const path = `
			M ${x} ${y}
			L ${x} ${y + h * yrvs}
			C  ${x} ${y + (h + r) * yrvs} ${x} ${y + (h + r) * yrvs} ${x + r * xrvs} ${y + (h + r) * yrvs}
			L ${x + (w + r) * xrvs} ${y + (h + r) * yrvs}
			C  ${ex}  ${y + (h + r) * yrvs} ${ex}  ${y + (h + r) * yrvs} ${ex} ${ey - h * yrvs}
			L ${ex} ${ey}`
		return path;
	}


	// Draws links from source node to destination node
	// https://observablehq.com/@venky18/curved-edges-vertical-d3-v3-v4-v5-v6
	function draw_multiparent_indicator(source, target) {
		const x = source.x + dimens.node_width / 2;
		const y = source.y + dimens.node_height / 2;
		const ex = target.x + dimens.node_width / 2;
		const ey = target.y + dimens.node_height / 2;

		let xrvs = ex - x < 0 ? -1 : 1;
		let yrvs = ey - y < 0 ? -1 : 1;

		let rdef = 35;
		let r = Math.abs(ex - x) / 2 < rdef ? Math.abs(ex - x) / 2 : rdef;

		r = Math.abs(ey - y) / 2 < r ? Math.abs(ey - y) / 2 : r;

		let h = Math.abs(ey - y) - r;
		let w = Math.abs(ex - x) - r * 2;
		//w=0;
		// Curve x1 y1 x2 y2 xy
		const path = `
            M ${x} ${y}
            L ${x} ${y+h*yrvs}
            C ${x} ${y+h*yrvs+r*yrvs} ${x} ${y+h*yrvs+r*yrvs} ${x+r*xrvs} ${y+h*yrvs+r*yrvs}
            L ${ex} ${ey}`

		return path;
	}

	var zoom_behaviours = d3.behavior
		.zoom()
		.scaleExtent(min_max_zoom_proportions)
		.on("zoom", redraw);

	d3.selection.prototype.appendHTML =
	d3.selection.enter.prototype.appendHTML = function(HTMLString) {
		return this.select(function() {
			return this.appendChild(document.importNode(new DOMParser().parseFromString(HTMLString, 'text/html').body.childNodes[0], true));
		});
	};

	d3.selection.prototype.appendSVG =
	d3.selection.enter.prototype.appendSVG = function(SVGString) {
		return this.select(function() {
			return this.appendChild(document.importNode(new DOMParser()
			.parseFromString('<svg xmlns="http://www.w3.org/2000/svg">' + SVGString + '</svg>', 'application/xml').documentElement.firstChild, true));
		});
	};

	var svg = d3.select('#' + current_svg_id)
		.append("svg")
		.attr('id', get_element_id('svgDiv'))
		.style('cursor', 'grab')
		.attr('width', dimens.chart_width)
		.attr('height', dimens.chart_height)
		.attr("top", 0)
		.call(zoom_behaviours)
		.append('g')
		.attr('id', get_element_id('drawArea'))
		.attr('transform', "translate(" + dimens.root_node_left_margin + "," + dimens.root_node_top_margin + ")");

	zoom_behaviours.translate([dimens.root_node_left_margin, dimens.root_node_top_margin]);

	hierarchy_data.x0 = 0;
	hierarchy_data.y0 = dimens.root_node_left_margin;
	if (params.mode != 'department') {
		// adding unique values to each node recursively		
		var uid = 1;
		add_node_property('node_id', function (v) {
			return uid++;
		}, hierarchy_data);
	}

	hierarchy_data = JSON.parse(JSON.stringify(hierarchy_data).replace(/"children":/g, '"kids":'));
	
	function add_page_number(d) {
		if (d && d.kids) {
			d.page = 1;
			d.children = [];            
			d.kids.forEach(function (d1, i) {
				d1.page_no = Math.ceil((i + 1) / nodes_per_page);
				if (d.page === d1.page_no) {
					d.children.push(d1)
				};
				add_page_number(d1);
			});
		}
	}
	
	add_page_number(hierarchy_data);

	expand_node(hierarchy_data);
	console.log(hierarchy_data);
	
	if (hierarchy_data.children) {
		hierarchy_data.children.forEach(collapse_node);
	}

	update_visualization(hierarchy_data);

	function update_visualization(source, param) {
		// Compute the new tree layout.
		var nodes = tree.nodes(hierarchy_data)
			.reverse(),
			links = tree.links(nodes);
		
		console.log(nodes, links);

		// Normalize for fixed-depth.
		nodes.forEach(function (d) {
			d.y = d.depth * link_line_size;
		});

		// Update the nodes…
		var node = svg.selectAll("g.node")
			.data(nodes, function (d) {
				return d.id || (d.id = ++temp_node_index);
			})

		var node_enter = node.enter()
			.append('g')
			.attr('class', "node")
			.attr('transform', function (d) {
				if(param && param.parent_page){
					return ("translate(" + param.parent_page[0] + "," + param.parent_page[1] + ")")
				}
				return "translate(" + source.x0 + "," + source.y0 + ")";
			})

		var node_group = node_enter.append('g')
			.attr('class', "node-group")            

		// Add parent rectangle to the node
		node_group.append("rect")
			.attr('width', dimens.node_width)
			.attr('height', dimens.node_height)
			.attr('id', function(d) {
				return get_element_id('node' + d.node_id);
			})
			.attr("data-node-group-id", function (d) {
				return get_element_id(d.node_id);
			})
			.attr('rx', '.5rem')
			.attr('fill', function (d) {
				if (d.hasOwnProperty(entity_relation_type_column)) {
					return (d[entity_relation_type_column].toLowerCase().indexOf("filled")) == -1? true_node_background : dummy_node_background;
				}

				return true_node_background;
				
			})
			.attr('class', function (d) {
				var res = "";
				if (d.isLoggedUser) res += 'nodeRepresentsCurrentUser ';
				res += d._children || d.children ? "nodeHasChildren" : "nodeDoesNotHaveChildren";
				return res;
			});

		node_group.append("circle")
			.attr('class', 'ghostCircle')
			.attr("r", 30)
			.attr("opacity", 0.2) // change this to zero to hide the target area
			.style('fill', "blue")
			.attr('pointer-events', 'mouseover')
			.on("mouseover", function (node) {
				overCircle(node);
			})
			.on("mouseout", function (node) {
				outCircle(node);
			});

		var node_config = config_data[config_section_node];
		var node_text_config = config_data[config_section_node][config_subsection_text];
			
		// Add title text to the node
		if (node_text_config[config_key_title_text].value != 'NULL' && node_text_config[config_key_title_text].value != '-') {
			node_group.append("text")
				.attr('x', dimens.node_text_left_margin)
				.attr('y', dimens.node_title_top_margin)
				.attr('class', 'node-title')
				.style('font-size', dimens.node_title_font_size)
				.attr("text-anchor", "left")
				.style('word-break', 'break-all')
				.text(function (d) {
					var title = to_title_case(get_text_from_template(d, node_text_config[config_key_title_text].value, node_text_config[config_key_title_text].parameters));

					if (title.length <= 70) {
						return title;
					}
					else {
						var title_trimmed = title.substring(0, 70);
						title_trimmed = title_trimmed.substring(0, title_trimmed.lastIndexOf(' ')) + ' ...'
						return title_trimmed;
					}
				})
				.call(wrap_text, (200 * dimens.node_width) / 270);
		};

		// Add subtitle to the node
		if (node_text_config[config_key_subtitle_text].value != 'NULL' && node_text_config[config_key_subtitle_text].value != '-') {
			node_group.append("text")
				.attr('x', dimens.node_text_left_margin)
				.attr('y', dimens.node_subtitle_top_margin)
				.attr('class', 'node-subtitle')
				.style('font-size', dimens.node_subtitle_font_size)
				.attr("dy", ".35em")
				.style('font-family', 'FontAwesome')
				.attr("text-anchor", "left")
				.text(function (d) {
					var subtitle = get_text_from_template(d, node_text_config[config_key_subtitle_text].value, node_text_config[config_key_subtitle_text].parameters);
					if (subtitle.length == 0 || subtitle.toLowerCase() === 'null' || subtitle == undefined) {
						return '';
					}

					var subtitle_trimmed = subtitle.substring(0, 27);
					if (subtitle_trimmed.length < subtitle.length) {
						subtitle_trimmed = subtitle_trimmed.substring(0, 24) + '...'
					}

				return subtitle_trimmed;
			})
		};
		//Add addiional Parent indicator to nodes
		node_group.append("path")
		.attr("d", function(d){
			return draw_link(
				{	x:dimens.multi_parent_indicator_source_x,
					y:-dimens.node_height+dimens.multi_parent_indicator_source_y
				},
				{	x:dimens.multi_parent_indicator_dest_x,
					y:-dimens.node_height+dimens.multi_parent_indicator_dest_y
				})
		})
		.attr('stroke-width',"1px")
		.attr('stroke',"grey")
		.style('fill', "none")
		.style('stroke-dasharray', "3 2")
		.style('display', function(d) {
			if (d.parent_nodes) {
				return (d.parent_nodes.length > 1 ? 'block' : 'none');
			}
			return ('none');
		});
		
		node_group.append("circle")
		.attr("r","3")
		.attr('cx', dimens.node_width/2+dimens.multi_parent_indicator_source_x)
		.attr('cy', -dimens.node_height/2 +(dimens.multi_parent_indicator_source_y) )
		.style('fill', "grey")
		.attr('stroke',"grey")
		.style('display', function(d) {
			if (d.parent_nodes) {
				return (d.parent_nodes.length > 1 ? 'block' : 'none');
			}
			return ('none');
		});

		var collapsiblesWrapper = node_enter.append('g')
				.attr('class', 'collapsible-circle')
				.attr('data-id', function (v) {
					return get_element_id(v.node_id);
				});

		var collapsibles =
			collapsiblesWrapper.append("circle")
				.attr('class', 'node-collapse')
				.attr('cx', dimens.node_width/2)
				.attr('cy', dimens.node_height)
				.attr("", setCollapsibleSymbolProperty);

		//hide collapse rect when node does not have children
		collapsibles.attr("r", function (d) {
			if (d.children || d._children) return ((dimens.collapse_circle_radius * dimens.node_width) / 270);
			return 0;
		})
		.attr('height', ((dimens.collapse_circle_radius * dimens.node_width) / 270))

		collapsiblesWrapper.append("text")
			.attr('class', 'text-collapse')
			.attr('x', dimens.node_width/2)
			.attr('y', dimens.node_height + ((4.5 * dimens.node_width) / 270))
			.attr('width', ((dimens.collapse_circle_radius * dimens.node_width) / 270))
			.attr('height', ((dimens.collapse_circle_radius * dimens.node_width) / 270))
			.style('font-size', ((dimens.collapible_font_size * dimens.node_width) / 270))
			.attr("text-anchor", "middle")
			.style('font-family', 'Fira Code')
			.style('font-weight', 'bold')
			.text(function (d) {
				if (d.children) {
					d.collapseText = COLLAPSE_SYMBOL;
				}
				return d.collapseText;
			})

		collapsiblesWrapper.on("click", click);

		// Add caption to the node
		if (node_text_config[config_key_caption_text].value != 'NULL' && node_text_config[config_key_caption_text].value != '-') {
			node_group.append("text")
				.attr('x', dimens.node_text_left_margin)
				.attr('y', dimens.node_caption_top_margin)
				.attr('class', 'node-caption')
				.attr("text-anchor", "left")
				.style('font-size', dimens.node_caption_font_size)
				.style('fill', '#272727')
				.text(function (d) {
					if (node_text_config[config_key_caption_text].parameters === '{Metric}') {
						if (selected_metric_to_display != null) {
							var caption_text = params.config_display_text[selected_metric_to_display] + ' : ';

							var formatted_value = d[selected_metric_to_display];
							if (values_to_shorten.includes(selected_metric_to_display)) {
								formatted_value = number_to_text(d[selected_metric_to_display]);
							}

							caption_text += (params.config_format_values[selected_metric_to_display].replace('%s', formatted_value))

							return caption_text;
						}
						else {
							return '';
						}
					}
					else {
						var column_template = node_text_config[config_key_caption_text].parameters;
						
						if (column_template.trim().indexOf(template_delimiter) == -1) {
							if (values_to_shorten.includes(column_template)) {
								var caption_text = params.config_display_text[column_template] + ' : ';
								var formatted_value = number_to_text(d[column_template]);

								caption_text += (params.config_format_values[column_template].replace('%s', formatted_value))
								return caption_text;
							}
						}
						
						return get_text_from_template(d, node_text_config[config_key_caption_text].value, column_template);
					}
				});
		}

		node_group.append("text")
		.attr('x', dimens.node_child_count_left_margin)
		.attr('y', dimens.node_child_count_top_margin)
		.attr('class', "child-count-icon")
		.attr("text-anchor", "left")
		.style('font-family', 'FontAwesome')
		.style('font-size', dimens.node_child_count_font_size)
		.text(function (d) {
			if (d.children || d._children) {
				var string = child_count_icon  + ' ' + d.kids.length;
				return (string);
			}
		});

		// Add the logo/icon to the node
		if (node_config[config_subsection_logo]['field'].value != '-' && node_config[config_subsection_logo]['field'].value.toLowerCase() != 'null') {
			node_group.append("defs").append("svg:clipPath")
				.attr('id', "clip")
				.append("svg:rect")
				.attr('id', "clip-rect")
				.attr("rx", 3)
				.attr('x', dimens.node_padding)
				.attr('y', 2 + dimens.node_padding)
				.attr('width', dimens.node_icon_width)
				.attr('fill', 'none')
				.attr('height', dimens.node_icon_height)

			node_group.append("svg:image")
				.attr('x', dimens.node_padding)
				.attr('y', 2 + dimens.node_padding)
				.attr('width', dimens.node_icon_width)
				.attr('height', dimens.node_icon_height)
				.attr('clip-path', "url(#clip)")
				.attr("xlink:href", function (d) {
					if (image_paths[config_section_node].hasOwnProperty(d[node_config[config_subsection_logo]['field'].value])) {
						var path = image_paths[config_section_node][d[node_config[config_subsection_logo]['field'].value]];
						return path;
					}
				});
		}

		// Transition nodes to their new position.
		var node_update = node.transition()
			.duration(transition_duration)
			.attr('transform', function (d) {
				return "translate(" + d.x + "," + d.y + ")";
			})

		//todo replace with attrs object
		node_update.select("rect")
			.attr('width', dimens.node_width)
			.attr('height', dimens.node_height)
			.attr('rx', 3)
			.style('stroke', function (d) {
					return node_stroke;
			})
			.style('stroke-width', function (d) {
				if (param && d.node_id == param.id_to_locate) {
					return dimens.selected_node_stroke_width;
				}
				return dimens.node_stroke_width
			})

		var node_exit = node.exit()
			.attr('opacity', 1)
			.transition()
			.duration(transition_duration)
			.attr('transform', function (d) {
				if(param && param.parent_page){
					return ("translate(" + param.parent_page[0] + "," + param.parent_page[1] + ")")
				
				}
				return ('translate(' + source.x + ',' + source.y + ')')                
			})
			.each('end', function() {
				d3.select(this).remove();
			})
			.attr('opacity', 0);
  
		node_exit.select("rect")
			.attr('width', dimens.node_width)
			.attr('height', dimens.node_height)

		// Update the links…
		var link = svg.selectAll("path.link")
			.data(links, function (d) {
				return d.target.id;
			});

		// Enter any new links at the parent's previous position.
		link.enter().insert("path", 'g')
			.attr('class', "link")
			.attr('x', dimens.node_width / 2)
			.attr('y', dimens.node_height / 2)
			.attr('stroke', function (d) {     
				if (Object.keys(params.relation_types).length == 0) {
                    return link_colors[0];
                }
                return params.relation_types[d.target[config_data[config_section_data][config_data_subsection_column_name][config_data_key_relation_type].value]];
			})
			.attr('stroke-width', function (d) {
				return "2px";
			})
			.attr("d", function (d) {
				if(param && param.parent_page){
					var o = {
						x: param.parent_page[0],
						y: param.parent_page[1]
					};
					return draw_link(o,o)
				}
				var o = {
					x: source.x,
					y: source.y
				};
				return draw_link(o,o);
			});

		// Transition links to their new position.
		link.transition()
			.duration(transition_duration)
			.attr("d", d => draw_link(d.source,d.target));

		// Transition exiting nodes to the parent's new position.
		link.exit().transition()
			.duration(transition_duration)
			.attr("d", function (d) {
				if(param && param.parent_page){
					var o = {
						x: param.parent_page[0],
						y: param.parent_page[1]
					};
					return draw_link(o,o)
				}
				var o = {
					x: source.x,
					y: source.y
				};
				return draw_link(o,o);
			})
			.remove();

		// Stash the old positions for transition.
		nodes.forEach(function (d) {
			d.x0 = d.x;
			d.y0 = d.y;
		});
		
		var parents = nodes.filter(function(d) {
			return (d.kids && d.kids.length > nodes_per_page) ? true : false;
		});
		
		svg.selectAll(".page").remove();
		svg.selectAll(".page-text").remove();

		parents.forEach(function(p) {
			if (p._children)
			  	return;
			var currPar = Object.assign({}, p);//helper for left right navigation position

			var paging_data = [];
			var page_text_data = [];

			if (p.page > 1) {
			  	paging_data.push({
					type: "prev",
					parent: p,
					no: (p.page - 1)
			  	});
			}
			if (p.page < Math.ceil(p.kids.length / nodes_per_page)) {
			  	paging_data.push({
					type: "next",
					parent: p,
					no: (p.page + 1)
			  	});
			}
			if (p.children && p.kids) {
				page_text_data.push({
					current_page: p.page,
					total_pages: Math.ceil(p.kids.length / nodes_per_page)
			  	});
			}
			
			var page_control = svg.selectAll(".page")
			  	.data(paging_data, function(d) {
					return (d.parent.id + d.type);
			  	}).enter()
			  	.append('g')
			  	.attr('class', "page")
			  	.attr('transform', function(d) {
					var x = (d.type == "next") ? currPar.x + ((260 * dimens.node_width) / 270) : currPar.x + ((10 * dimens.node_width) / 270);
					var y = currPar.y + ((97 * dimens.node_width) / 270);
					return "translate(" + x + "," + y + ")";
				})
				.on("click", paginate);
		  
			page_control
			  	.append("circle")
			  	.attr("r", ((13 * dimens.node_width) / 270))
				.style('fill', "#eeeeee");
				  
			page_control
			  	.append("image")
			  	.attr("xlink:href", function(d) {
					if (d.type == "next") { return icon_path_next; } 
					else { return icon_path_previous; }
			  	})
			  	.attr('x', ((-7 * dimens.node_width) / 270))
			  	.attr('y', ((-7 * dimens.node_width) / 270))
			  	.attr('width', ((15 * dimens.node_width) / 270))
			  	.attr('height', ((15 * dimens.node_width) / 270));

			var page_text_control = svg.selectAll(".page-text")
				.data(page_text_data)
				.enter()
				.append('g')
				.attr('class', "page-text")
		   		.attr('transform', function(d) {
					var x = currPar.x + ((150 * dimens.node_width) / 270); 
					var y = currPar.y + ((100 * dimens.node_width) / 270);
					return "translate(" + x + "," + y + ")";
				})
			
			page_text_control.append("text")
				.style('font-size', ((14 * dimens.node_width) / 270))
				.text(function (d) {
					return "Page " + d.current_page + " / " + d.total_pages;
				})
		});
		  
		function paginate(d,) {
			d.parent.page = d.no;
			
			set_page(d.parent);
			update_visualization(hierarchy_data,{
				parent_page: [d.parent.x0, d.parent.y0]
			});
		}
		
		function set_page(d) {
			if (d && d.kids) {
			  	d.children = [];
			  	d.kids.forEach(function(d1, i) {
					if (d.page === d1.page_no) {
						collapse_node(d1);
						d.children.push(d1);
					}
			  	})
			}
		}

		function sidebar_handler(d) {
			var content = get_sidebar_script(d)
			if (selected_node_id != "") {
				d3.select(selected_node_id).style('stroke-width', dimens.node_stroke_width)
			}

			selected_node_id = get_element_id("node" + d.node_id, true);

			document.getElementById(html_sidebar_id).innerHTML = content;

			d3.select(selected_node_id).style('stroke-width', dimens.selected_node_stroke_width);
		}

		if (document.getElementById(html_sidebar_id).style.display != 'none') {
			node_group.on('click', sidebar_handler);
		}
	}

	function click(d) {
		d3.select(this).select("text").text(function (dv) {

			if (dv.collapseText == EXPAND_SYMBOL) {
				dv.collapseText = COLLAPSE_SYMBOL
			} else {
				if (dv.children) {
					dv.collapseText = EXPAND_SYMBOL
				}
			}
			return dv.collapseText;
		})

		if (d.children) {
			d._children = d.children;
			d.children = null;
		} else {
			d.children = d._children;
			d._children = null;
		}
		update_visualization(d);
	}

	function redraw() {
		svg.attr('transform',
			"translate(" + d3.event.translate + ")" +
			" scale(" + d3.event.scale + ")");
	}

	function wrap_text(text, width) {
		text.each(function () {
			var text = d3.select(this),
				words = text.text().split(/\s+/).reverse(),
				word,
				line = [],
				lineNumber = 0,
				lineHeight = 1.2,
				x = text.attr('x'),
				y = text.attr('y'),
				dy = 0,
				tspan = text.text(null)
					.append("tspan")
					.attr('x', x)
					.attr('y', y)
					.attr("dy", dy + "em");
					
			while (word = words.pop()) {
				line.push(word);
				tspan.text(line.join(" "));
				if (tspan.node().getComputedTextLength() > width) {
					line.pop();
					tspan.text(line.join(" "));
					line = [word];
					tspan = text.append("tspan")
						.attr('x', x)
						.attr('y', y)
						.attr("dy", ++lineNumber * lineHeight + dy + "em")
						.text(word);
				}
			}
		});
	}

	function add_node_property(propertyName, propertyValueFunction, element) {
		if (!element[propertyName]) {
			element[propertyName] = propertyValueFunction(element);
		}
		if (element.children) {
			element.children.forEach(function (v) {
				add_node_property(propertyName, propertyValueFunction, v)
			})
		}
		if (element._children) {
			element._children.forEach(function (v) {
				add_node_property(propertyName, propertyValueFunction, v)
			})
		}
	}

	function show_search_results(results) {
		var config = config_data[config_section_searchbar];

		var search_script_array = results.map(function (result) {
			var script = '';

            script += `<div class="list-item" style="cursor: pointer;" onclick='params.functions.find_node(${result.node_id})'>`;
            script += `<a>`;
            script += `<div class="image-wrapper">`;

            // Add icon in search results
            if (config[config_subsection_logo]['field'].value != '-' && config[config_subsection_logo]['field'].value.toLowerCase() != 'null') {
                var path = image_paths[config_section_searchbar][result[config[config_subsection_logo]['field'].value]];
                script += `<img class="image" src="${path}">`
            }

            script += `</div>`;
            script += `<div class="description">`;

            var text_section = config[config_subsection_text];

            // Add title text in search results
            if (text_section[config_key_title_text].value != '-' && text_section[config_key_title_text].value.toLowerCase() != 'null') {
                var title = to_title_case(get_text_from_template(result, text_section[config_key_title_text].value, text_section[config_key_title_text].parameters));
                script += `<div class="name">${title}</div>`
            }

            // Add subtitle text in search results
            if (text_section[config_key_subtitle_text].value != '-' && text_section[config_key_subtitle_text].value.toLowerCase() != 'null') {
                var subtitle = to_title_case(get_text_from_template(result, text_section[config_key_subtitle_text].value, text_section[config_key_subtitle_text].parameters));
                script += `<div class="entity-type">${subtitle}</div>`
            }

            // Add caption text in search results
            if (text_section[config_key_caption_text].value != '-' && text_section[config_key_caption_text].value.toLowerCase() != 'null') {
                var caption = to_title_case(get_text_from_template(result, text_section[config_key_caption_text].value, text_section[config_key_caption_text].parameters));
                script += `<p class="area">${caption}</p>`;
            }

            script += `</div>`;
            script += `<div class="buttons">`;
            script += `</div>`;
            script += `</a>`;
            script += `</div>`;

            return script;
		});

		var search_script = search_script_array.join('');
		clear_search_results();

		var parent_element = get('.result-list');
		var old = parent_element.innerHTML;
		var new_element = search_script + old;
		parent_element.innerHTML = new_element;
		set('.user-search-box .result-header', "RESULT - " + search_script_array.length);
	}

	function find_node(uid) {
		params.functions.locate_node[current_visualization_index](uid);
	}

	function searchbar_handler() {
		var input = get('.user-search-box .search-input');
		var value = input.value ? input.value.trim() : '';
		

			if (value.length < 3) {
				clear_search_results();
			} 
			else {
				var searchResult = params.functions.find_in_tree[current_visualization_index](hierarchy_data, value);
				params.functions.show_search_results[current_visualization_index](searchResult);
			}
	}

	function searchbar_listener() {
		var input = get('.user-search-box .search-input');

		input.addEventListener('input', function () {
			if (current_visualization_index == svg_index) {
				params.functions.search_entity[current_visualization_index]();
			}
		});
	}
	
	function fit_to_screen(paddingPercent = 0.95, transitionDuration=500) {
		var root = d3.select(get_element_id('drawArea', true));
		var bounds = root.node().getBBox();
		var parent = root.node().parentElement;
		var fullWidth = parent.clientWidth,
			fullHeight = parent.clientHeight;
		var width = bounds.width,
			height = bounds.height;
		var midX = bounds.x + width / 2,
			midY = bounds.y + height / 2;
		if (width == 0 || height == 0) return;
		var scale = (paddingPercent || 0.75) / Math.max(width / fullWidth, height / fullHeight);
		var translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY];
	
		console.trace("zoomFit", translate, scale);
		root.transition()
			.duration(transitionDuration || 0)
			.call(zoom_behaviours.translate(translate).scale(scale).event);
	}

	function findInTree(rootElement, searchText) {
		var result = [];
		var regex_search_word = new RegExp(searchText, "i");

		recursivelyFindIn(rootElement, searchText.toLowerCase());
		return result;

		function recursivelyFindIn(user, regex_search_word) {
			if (user[config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_name].value].indexOf('(Filled)') == -1) {
				entity_name = user[config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_name].value].toLowerCase()
				if (entity_name.indexOf(regex_search_word) != -1) {                    
					//window.alert("1")
					result.push(user);
				}
			}

			//var childUsers = user.children ? user.children : user._children;
			var childUsers = user.kids;
			if (childUsers) {
				childUsers.forEach(function (childUser) {
					recursivelyFindIn(childUser, regex_search_word)
				})
			}
		};
	}

	function expand_all_nodes() {
		expand_node(hierarchy_data, true);
		update_visualization(hierarchy_data);
	}

	function expand_node(d, expand_all=false) {
		if (d.children) {
			setToggleSymbol(d, COLLAPSE_SYMBOL);
			d.children.forEach(function(d) {
				expand_node(d, expand_all)
			});
		}

		if (d._children) {
			setToggleSymbol(d, COLLAPSE_SYMBOL);
			d.children = d._children;
			if (expand_all) {
				d.children.forEach(function(d) {
					expand_node(d, expand_all)
				});
			}
			d._children = null;
		}

		if (d.children) {
			setToggleSymbol(d, COLLAPSE_SYMBOL);
		}
	}
	
	function collapse_all_nodes() {
		if (hierarchy_data.children) {
			hierarchy_data.children.forEach(collapse_node);
		}
		update_visualization(hierarchy_data);
	}

	function collapse_node(d) {
		if (d._children) {
			d._children.forEach(collapse_node);
		}
		if (d.children) {
			d._children = d.children;
			d._children.forEach(collapse_node);
			d.children = null;
		}

		if (d._children) {
			setToggleSymbol(d, EXPAND_SYMBOL);
		}
	}

	function setCollapsibleSymbolProperty(d) {
		if (d._children) {
			d.collapseText = EXPAND_SYMBOL;
		} else if (d.children) {
			d.collapseText = COLLAPSE_SYMBOL;
		}
	}

	function setToggleSymbol(d, symbol) {
		d.collapseText = symbol;
		d3.select("[data-id='" + get_element_id(d.node_id) + "']").select('text').text(symbol);
	}

	function locate_node(id) {
		var copy_object = _.cloneDeep(hierarchy_data)
		expand_node(copy_object)
		var temp = recursive_search(copy_object, id,[]);

		var  final_path = Array.from(new Set(temp))
		final_path.shift()

		if (hierarchy_data.children) {
			hierarchy_data.children.forEach(collapse_node);
		}
		
		expand_selected_nodes(hierarchy_data, final_path, id)

		update_visualization(hierarchy_data, {
			id_to_locate : id
		});
 
		var elem = document.getElementById(get_element_id("node" + id))
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
	}
	
	var overCircle = function (d) {
		selectedNode = d;
		update_temp_connector();
	};
	
	var outCircle = function (d) {
		selectedNode = null;
		update_temp_connector();
	};

	// Function to update the temporary connector indicating dragging affiliation
	var update_temp_connector = function () {
		var data = [];
		if (draggingNode !== null && selectedNode !== null) {
			// have to flip the source coordinates since we did this for the existing connectors on the original tree
			data = [{
				source: {
					x: selectedNode.x0,
					y: selectedNode.y0
				},
				target: {
					x: draggingNode.x0,
					y: draggingNode.y0
				}
			}];
		}
		var link = svg.selectAll(".templink").data(data);
		
		link.enter().append("path")
			.attr('class', "templink")
			.attr("d", d3.svg.diagonal())
			.attr('pointer-events', 'none');

		link.attr("d", draw_link(d.source,d.target));

		link.exit().remove();
	};

	// Recursively search for the onde with given id in the hierarchy
	function recursive_search(node, value, path){    
		if (node.node_id == value){
			path.push(node);
			return path;
		}
		else if (node.kids){
			for (let index = 0; index < node.kids.length; index+=1) {
				path.push(node);
				var fo = recursive_search(node.kids[index], value, path);
			 
				if(fo) {
					return fo;
				}
				else {
					path.pop();
				}
			}
		
		  	if (node.children && node.children[node.children.length - 1].node_id != node.kids[node.kids.length - 1].node_id) {
				var currPageNo  = node.children[0].page_no;
				node.children = node.kids.filter( kid => kid.page_no == currPageNo + 1);

				path.push(node);
				var fo = recursive_search(node,value,path);
				if(fo) {
					return fo;
				}
				else {
					path.pop();
				}
		  	}
		  	else {
				return false;
			}
		}
		else {
			return false;
		}
	}

	// Collapse the tree and expand the nodes in the path
	function expand_selected_nodes(node, path) {                 
		while(path.length > 0) {
			node.children = node.kids.filter( kid => kid.page_no == path[0].page_no);
			node = node.children.filter(next_node => next_node.node_id == path[0].node_id)[0];
			expand_node(node);           
			path.shift();
		}
	};

	var elem = document.getElementById(get_element_id('node' + hierarchy_data.node_id));
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

	// Returns html script for the details sidebar
	function get_sidebar_script(item) {
		if (svg_index == primary_visualization_index) {
			node_selected_in_primary_visualization = item.node_id;
		}

		var config = config_data[config_section_sidebar];
		var images = image_paths[config_section_sidebar];

		var script = '';
		script += '<div class="ui sidebarcard">';
		script += '<div class="detailsBar">';

		// Add icon in sidebar
		if (config[config_subsection_logo]['field'].value != '-' && config[config_subsection_logo]['field'].value.toLowerCase() != 'null') {
			if (images.hasOwnProperty(item[config[config_subsection_logo]['field'].value])) {
				var path = images[item[config[config_subsection_logo]['field'].value]];
				script += `<img class="left floated mini ui image" src="${path}" style="height:  40px;">`;
			}
		}

		var text_section = config[config_subsection_text];

		// Add title text in sidebar
		if (text_section[config_key_title_text].value != '-' && text_section[config_key_title_text].value.toLowerCase() != 'null') {
			var title = to_title_case(get_text_from_template(item, text_section[config_key_title_text].value, text_section[config_key_title_text].parameters));
			script += `<div class="header" style="font-size: ${dimens.sidebar_title_font_size}px;"><b>${title}</b></div>`;
		}

		// Add subtitle text in sidebar
		if (text_section[config_key_subtitle_text].value != '-' && text_section[config_key_subtitle_text].value.toLowerCase() != 'null') {
			var subtitle = to_title_case(get_text_from_template(item, text_section[config_key_subtitle_text].value, text_section[config_key_subtitle_text].parameters));
			script += `<div class="meta" style="font-size: ${dimens.sidebar_subtitle_font_size}px;">${subtitle}</div>`;
		}

		// Add caption text in sidebar
		if (text_section[config_key_caption_text].value != '-' && text_section[config_key_caption_text].value.toLowerCase() != 'null') {
			var caption = to_title_case(get_text_from_template(item, text_section[config_key_caption_text].value, text_section[config_key_caption_text].parameters));
			script += `<div class="meta" style="font-size: ${dimens.sidebar_caption_font_size}px;">${caption}</div>`;
		}

		// Add links in sidebar
		if (config.hasOwnProperty(config_subsection_link)) {
			for (let link in config[config_subsection_link]) {
				var link_text = config[config_subsection_link][link].value;
				var link_parameters = config[config_subsection_link][link].parameters;
				var link_href = config[config_subsection_link][link].content;

				var link_script = `<a style="font-size: ${dimens.sidebar_caption_font_size}px;" target="_blank" href="${get_text_from_template(item, link_href, link_parameters)}">${to_title_case(get_text_from_template(item, link_text, link_parameters))}</a>`;
				script += `<div>${link_script}</div>`;
			}
		}

		script += `<div class="description" style='margin-top: 15px;'>`;

		// Check if subhierarchy button is to be displayed
		if (show_sub_hierarchy_button == true) {
			// Check if the criteria for the button is met
			if (svg_index == primary_visualization_index && item.node_id != 1 && item.hasOwnProperty('kids')) {
				script += `<button class="ui teal basic button" style='width: 100%; line-height: ${25 * dimens.node_width/270}px; font-size: ${15 * dimens.node_width/270}px; text-align:center; float:center; padding: 4px;' onclick='show_subhierarchy(${item.node_index})'>Show Sub Hierarchy</button>`
			}
		}

		var is_section_valid = false;

		// Add details with categories
		if (config.hasOwnProperty(config_subsection_details)) {

			if (item.hasOwnProperty('parent_nodes')) {
				script += `<div class="ui divider"></div>`;
				script += `<div style="font-size: ${dimens.sidebar_content_font_size}px;"><b>Parents</b></div>`;
				item.parent_nodes.forEach(parent => {
					let parent_style = '';
					console.log(parent);
					if (Array.from(primary_flag_true).includes(parent[primary_flag_key]) && item.parent_nodes.length > 1) {
						parent_style = 'font-weight: bold;';
					}
					script += `<br><div style="font-size: ${dimens.sidebar_content_font_size}px; ${parent_style}">
					${to_title_case(parent[config_data[config_section_data][config_data_subsection_column_name][config_data_key_entity_name].value])}</div>`
				});
			}
			
			if (config[config_subsection_details].hasOwnProperty(config_key_categories)) {
				
				var categories_dict = config[config_subsection_details][config_key_categories];
				script += `<div class="ui divider"></div>`;
				
				var categories = categories_dict.value.split('|');
				
				// Iterate over all categories
				for (let i = 0; i < categories.length; i+=1) {
					var category = categories[i].trim();
					
					if (params.config_category_list.hasOwnProperty(category)) {
						is_section_valid = false;
						
						// Iterate over all fields in the selected category
						for (let j = 0; j < params.config_category_list[category].length; j+=1) {
							
							var field = params.config_category_list[category][j];

							if (is_section_valid == true) { script += '<br>'; }

							var field_value = item[field];
							
							if (values_to_shorten.includes(field)) {
								field_value = number_to_text(item[field]);
							}
							
							// Bring the value of the field in required format
							field_value = params.config_format_values[field].replace('%s', field_value);
							
							if (field_value == '' || field_value.toLowerCase() === 'null' || field_value.toLowerCase() === 'undefined') { continue; }
							
							// If value is to long, trim it
							var value_text = field_value.substring(0, 20);
							if (value_text.length < field_value.length) {
								value_text = value_text.substring(0, 17) + '...'
							}
							
							var metric_style = '';

							// If the metric is the selected metric, highlight it
							if (field === selected_metric_to_display) {
								metric_style = 'color: #FF0000; font-weight: bold;';
							}
							
							script += `<div style="font-size: ${dimens.sidebar_content_font_size}px; ${metric_style}">${params.config_display_text[field]}</span> <span class= "right-float"><b>${value_text}</b></span> </div>`
							is_section_valid = true;
							
						}

						if (is_section_valid == true) { script += `<div class="ui divider"></div>`; }

						
					}
					else {
						throw_error('Invalid category ' + category + ' in config file.', 'Please check whether the category exists in the columns file.')
					}
				}
			}
		}

		script += `</div>`;

		script += `</div>`;

		return script;
	};
}