from dagster import graph, op

@op
def prepare():
    return 'prepare'

@op
def backfill():
    return 'backfill'

@op
def sync():
    return 'sync'

@op
def validate():
    return 'validation'

@op
def cutover():
    return 'cutover'

@op
def phased_cutover_domains():
    return 'cutover'

@graph
def migration_graph():
    prepare()
    backfill()
    sync()
    validate()
    cutover()
    phased_cutover_domains()
