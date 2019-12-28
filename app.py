import os
import plaid
import utilities
import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from dotenv import load_dotenv
import plotly.graph_objects as go

from plaid.errors import APIError, ItemError
from plaid import Client

load_dotenv()

exclusions = os.getenv('EXCLUDE_CAT').split(',')
exclusions = [x.strip(' ') for x in exclusions]

hawk_mode = str(os.getenv('HAWK_MODE'))
print('Currently Running in {}'.format(hawk_mode))

start_date = "2019-10-01"

master_data = utilities.getData(hawk_mode, exclusions, start_date)

clean_data = utilities.dataPrep(master_data['all_trnsx'], exclusions)

bubble_data, bubble_sizeref = utilities.bubbleData(clean_data)
bubble_fig = utilities.bubbleFig(bubble_data, bubble_sizeref)

stack_data = utilities.stackData(clean_data)
stack_fig = utilities.stackFig(stack_data)

feline_data = utilities.felineData(clean_data)
feline_fig = utilities.felineFig(feline_data)

name_line_data = utilities.nameLineData(clean_data)
name_line_fig = utilities.nameLineFig(name_line_data)

rel_data = utilities.relativeData(clean_data)
rel_fig = utilities.relativeFig(rel_data)

df = utilities.transactionTables(master_data['all_trnsx'], start_date, exclusions, hawk_mode)

app = dash.Dash(__name__)
# server = app.server

def serve_layout():
    return html.Div([
        dcc.Graph(
            id='Bubble',
            figure=bubble_fig
        ),
        dcc.Graph(
            id='Stack',
            figure=stack_fig
        ),
        dcc.Graph(
            id='FeLine',
            figure=feline_fig
        ),
        dcc.Graph(
            id='NameLine',
            figure=name_line_fig
        ),
        dcc.Graph(
            id='Relative',
            figure=rel_fig
        ),
        dash_table.DataTable(
            id='datatable-interactivity',
            columns=[
                {"name": i, "id": i, "selectable": True} for i in df.columns
            ],
            data=df.to_dict('records'),
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable="multi",
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current= 0,
            page_size= 100,
        ),
        html.Div(id='datatable-interactivity-container'
        )
    ])


app.layout = serve_layout


@app.callback(
    Output('datatable-interactivity', 'style_data_conditional'),
    [Input('datatable-interactivity', 'selected_columns')]
)
def update_styles(selected_columns):
    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]

@app.callback(
    Output('datatable-interactivity-container', "children"),
    [Input('datatable-interactivity', "derived_virtual_data"),
     Input('datatable-interactivity', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []

    dff = df if rows is None else pd.DataFrame(rows)

    colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff))]

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff["Category_0"],
                        "y": dff[column],
                        "type": "bar",
                        "marker": {"color": colors},
                    }
                ],
                "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                    "height": 250,
                    "margin": {"t": 10, "l": 10, "r": 10},
                },
            },
        )
        # check if column exists - user may have deleted it
        # If `column.deletable=False`, then you don't
        # need to do this check.
        for column in ["pop", "lifeExp", "gdpPercap"] if column in dff
    ]



if __name__ == '__main__':
    app.run_server(debug=True)
