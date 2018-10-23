# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 13:26:33 2016

@author: james.bradley
"""

from copy import *
from gurobipy import *
import MySQLdb as mySQL
import datetime

def set_cover(set_all, subsets):
    set_cover = []
    indices = []
    num_subsets = 0
    subset_id_points = []
    temp = deepcopy(subsets)

    #print len(set_all), len(set_cover)

    while len(set_all) > len(set_cover):
        max_add = 0
        list_ind = 0
        i=0
        del_ind = []
        for thisList in temp:
            #print thisList
            num_add_ele = len(list(set(thisList[1]) - set(set_cover)))
            #print num_add_ele, i, thisList
            if num_add_ele > max_add:
                list_ind = i
                max_add = num_add_ele
            #if num_add_ele == 0:
            #    del_ind.append(i)
            i = i + 1
        del_ind.append(list_ind)
        indices.append(temp[list_ind][0])
        
        #print list_ind, temp[list_ind]
        num_subsets = num_subsets + 1
        for j in range(len(temp[list_ind][1])):
            if temp[list_ind][1][j] not in set_cover:
                set_cover.append(temp[list_ind][1][j])
                subset_id_points.append(tuple((temp[list_ind][0],temp[list_ind][1][j])))
        #print temp
        for k in reversed(del_ind):
            print k
            del temp[k]
            
        print 'towers added: ' + str(len(indices)) + '     numCalls: ' + str(len(set_cover))
                         
    print temp
    #print set_cover
    return num_subsets, indices, subset_id_points


start_prog = 1
start_time = datetime.datetime.now()
#import mysql.connector
#cnx = mysql.connector.connect(user='Jim', password='MySQL',
#                              host='127.0.0.1',
#                              database='networkopttest')
print 'Setting DB connection'
cnx = mySQL.connect(user='Jim', passwd='MySQL',
                    host='127.0.0.1', db='networkopttest')                             

# get parameters from database
cursor = cnx.cursor()
query = "CALL `spGetParameters`();"
cursor.execute(query)
query_results = list(cursor.fetchall())
# Parameters to be loaded form database table 'parameters'into a dictionary
    # cell_tower_fixed_cost       # fixed cost of cell tower added increment of capacity to be read from database
    # cell_tower_cap_inc_invest   # variable cost of cell tower added increment of capacity to be read from database
    # cell_tower_fixed_cap        # number of calls that a cell tower with base capacity can handle to be read from the database
    # cell_tower_add_cap          # number of addtional calls that a cell tower can handle  with each incremental unit of capacity
    # cell_tower_cap_inc_ub       # upper limit on th enumber of incremental capacity units for cell towers
    # originLatRad                # origin latitude of image in radians
    # originLongRad               # origin longitude of image in radians
    # cell_tower_fixed_cap
    # cell_tower_add_cap

parameters = {}
for i in range(len(query_results)):
    parameters[query_results[i][0]] = query_results[i][1]
cursor.close()

# Get the year represented in the database table 
#print '2nd query'
cursor = cnx.cursor()
cursor.execute("CALL `spGetYears`();")
theseYears = list(cursor.fetchall())
for i in range(len(theseYears)):         # convert list of tuples theseYears to list of values
    theseYears[i] = theseYears[i][0]
cursor.close()

# this loop is for preparing the feasible call-cell tower combinations for all the years:
#print '3rd query'
cursor = cnx.cursor()
cursor.execute("CALL spGetCellTowerLatLongRad()")
cell_towers = list(cursor.fetchall())
cursor.close()

#print 'execute stored procedure'
cursor = cnx.cursor()
cursor.callproc('spDeleteCombosAll')
cursor.callproc('spDeleteResultsTowerCapTable')
cursor.callproc('spDeleteFromOptResultsTable')
cursor.callproc('spDeleteTableauDataAll')
cnx.commit()

legacy_towers = []

#print 'completed stored procedure'

print 'Call tower and years data read'

for iYear in range(len(theseYears)):
    for iTower in range(len(cell_towers)):
        print theseYears[iYear], "   ", cell_towers[iTower]
        args = (parameters['originLatRad'], parameters['originLongRad'],  cell_towers[iTower][3],  cell_towers[iTower][4], theseYears[iYear], cell_towers[iTower][0])
        cursor.callproc('spFindCallsInRangeLatLong',args)        
        # query = ("""CALL `spFindCallsInRangeLatLong`(%s, %s, %s, %s, %s);""",(originLatRad, originLongRad, cell_towers[iTower][3], cell_towers[iTower][4], theseYears[iYear]))
        # cursor.execute(query)
        
cursor.close()

for thisYear in theseYears:
    # Get data from database table regarding allowed call-cell tower pairings
    cursor = cnx.cursor()
    cursor.execute("CALL spFeasibleCellCallCombosByYear(%s)" % thisYear)
    tower_call_pairs = list(cursor.fetchall())      # 2-tuples, 1st member is cell_tower_id, 2nd member is cell_loc_id
    cursor.close()
    
    # Get data form databse table regarding cell tower labels
    cursor = cnx.cursor()
    cursor.execute("CALL spGetFeasibleTowers(%s)" % thisYear)
    query_result = list(cursor.fetchall())                           # set of cell tower indices included in tower_call_pairs from database stored procedure
    towers = []
    for thisRecord in range(len(query_result)):
        towers.append(query_result[thisRecord][0])                          
    cursor.close()
    
    # Get data from database regarding call location labels
    cursor = cnx.cursor()
    cursor.execute("CALL spGetCallLocationIDs(%s)" % thisYear) 
    call_locs = []
    query_result = list(cursor.fetchall())                        # set of call location indices included in tower_call_pairs from database stored procedure
    for thisRecord in range(len(query_result)):
        call_locs.append(query_result[thisRecord][0])                          
    cursor.close()
  
    
    # create Gurobi model
    m = Model("network_opt")
    m.ModelSense = GRB.MINIMIZE
    #m.setParam('TimeLimit',7200)
    m.setParam('MIPGap',0.05)
    
    
    
    # create decision variables for cell towers
    # put decision variables in dictionaries for east referencing
    x_cell_tower = {}
    x_cell_cap_add = {}
    for tower in towers:
        x_cell_tower[tower] = m.addVar(vtype=GRB.BINARY,obj=parameters['cell_tower_fixed_cost'],name='x_cell_T'+str(tower))
        x_cell_cap_add[tower] = m.addVar(ub=parameters['cell_tower_cap_inc_ub'],vtype=GRB.INTEGER,obj=parameters['cell_tower_cap_inc_invest'],name='x_cell_xcap_T'+str(tower))
        
    x_cell_call_assign = {}
    for (tower, call) in tower_call_pairs:
        # this syntax works for adding an entry into a dictionary
        x_cell_call_assign[(tower, call)] = m.addVar(vtype=GRB.BINARY,name='x_tower_call_assign_T'+str(tower)+"_C"+str(call))
        
    # Obective function -- obj already specifies in decision variable declaration
    
    m.update()
    
    # Execute set-cover solution for first loop
    if start_prog == 1:
        cursor = cnx.cursor()
        query = "CALL `spGetFeasibleTowers`(%s);" % thisYear
        cursor.execute(query)
        temp_results = cursor.fetchall()
        feasible_towers=[]
        for thisTower in temp_results:
            feasible_towers.append(thisTower[0])
        cursor.close()
        
        subsets = []
        
        for thisTowerID in feasible_towers:
            cursor = cnx.cursor()
            query = "CALL `spGetCallsForTower`(%s, %s);" % (thisYear, thisTowerID)
            cursor.execute(query)
            temp_results = cursor.fetchall()
            feasible_cell_call_combos=[]
            for thisCall in temp_results:
                feasible_cell_call_combos.append(thisCall[0])
            subsets.append([thisTowerID,feasible_cell_call_combos])
            cursor.close()
        
        cursor = cnx.cursor()
        query = "CALL `spGetCallLocationIDs`(%s);" % thisYear
        cursor.execute(query)
        callIDs = []
        temp_results = cursor.fetchall()
        for thisCallLoc in temp_results:
            callIDs.append(thisCallLoc[0])
        cursor.close()
        
        # execute set-cover greedy algorithm
        num_towers, start_towers, start_soln = set_cover(callIDs,subsets)
        print 'CallIDs'
        for i in callIDs:
            print i
        print
        print 'subsets'
        for i in subsets:
            print i
        print
        print 'num_towers: ' + str(num_towers)
        print
        print 'start_towers: '
        for i in start_towers:
            print i
        print
        print 'start_soln: '
        for i in start_soln:
            print i
    
        print
        print 'len(start_soln): ' + str(len(start_soln))
        print
        print "Setting starting solution"
        # Set variable starting values
        for tower in towers:
            if tower in start_towers:
                x_cell_tower[tower].Start = 1
                print 'x_cell_tower[' + str(tower) + '] = 1'
                
        print
        print
                
        for (tower, call) in tower_call_pairs:
            if (tower, call) in start_soln:
                x_cell_call_assign[(tower, call)].Start = 1
                print 'x_cell_call_assign[(' + str(tower) + ', ' + str(call) + ')] = 1'
                
        print
        print
                
    start_prog = 0
                
    # Subject to:
    x_leg_tower_constr = {}
    x_leg_tower_cap_add_constr = {}
    for lt in legacy_towers:
        x_leg_tower_constr[lt[0]] = m.addConstr(x_cell_tower[lt[0]], GRB.EQUAL, 1,"LegacyTower_"+str(lt[0]))
        #m.addConstr(x_cell_tower[lt[0]], GRB.EQUAL, 1,"LegacyTower_"+str(lt[0]))
        x_leg_tower_cap_add_constr[lt[0]] = m.addConstr(x_cell_cap_add[lt[0]], GRB.GREATER_EQUAL, lt[1],"LegacyTowerCapAdd_"+str(lt[0]))
        m#.addConstr(x_cell_cap_add[lt[0]], GRB.GREATER_EQUAL, lt[1],"LegacyTowerCapAdd_"+str(lt[0]))
    m.update()
     
    for tower in towers:
        print 'Tower constraints: '+str(tower)
        m.addConstr(quicksum(x_cell_call_assign[(tower,call)] for (tower, call) in [tc for tc in tower_call_pairs if tc[0] == tower]),GRB.LESS_EQUAL, 
                    x_cell_tower[tower] * parameters['cell_tower_fixed_cap'] + parameters['cell_tower_add_cap'] * x_cell_cap_add[tower],"CellTowerCap_T"+str(tower))
        m.addConstr(x_cell_cap_add[tower],GRB.GREATER_EQUAL, 0,"CellTowerCapAddMin_T"+str(tower))
                    
    m.update()
    
    for call in call_locs:
        print 'Call constraint: '+str(call)
        m.addConstr(quicksum(x_cell_call_assign[(tower,call)] for (tower, call) in [tc for tc in tower_call_pairs if tc[1] == call]),GRB.EQUAL, 1,"CallsHandled_C"+str(call))
     
    for (tower, call) in tower_call_pairs:
        print 'Tower-Call constraint: '+str(tower)+'  '+str(call)
        m.addConstr(x_cell_call_assign[(tower,call)],GRB.LESS_EQUAL,x_cell_tower[tower],"NoTowerNoCall_T"+str(tower)+"_C"+str(call))
        
    #for tower in towers:
    #   print 'Tower constraint: '+str(tower)
    #   m.addConstr(x_cell_cap_add[tower],GRB.GREATER_EQUAL, 0,"CellTowerCap_T"+str(tower))
        
    m.update()
    
    flog = open('E:\TeachingMaterials\BusinessAnalytics\ProblemsExamples\VerizonCellTowerGurobi\GurobiLogFile.txt', 'a')
    flog.write('Parameter Log @'+str(datetime.datetime.now())+'\n')
    flog.write('MIPGap: '+str(m.params.MIPGap)+'\n')
    flog.write('Threads: '+str( m.params.Threads)+'\n')
    flog.write('MIPFocus: '+str( m.params.MIPFocus)+'\n')
    flog.write('ImproveStartTime: '+str( m.params.ImproveStartTime)+'\n')
    flog.write('ImproveStartGap: '+str( m.params.ImproveStartGap)+'\n')
    flog.write('TimeLimit: '+str(m.params.TimeLimit)+'\n')
    flog.write('MIPGapAbs: '+str(m.params.MIPGapAbs)+'\n')
    flog.write('NodeLimit: '+str( m.params.NodeLimit)+'\n')
    flog.write('IterationLimit: '+str(m.params.IterationLimit)+'\n')
    flog.write('SolutionLimit: '+str(m.params.SolutionLimit)+'\n')
    flog.write('Cutoff: '+str(m.params.Cutoff)+'\n')
    flog.write('NodefileStart: '+str(m.params.NodefileStart)+'\n') 
    flog.write('NodefileDir: '+str(m.params.NodefileDir)+'\n') 
    flog.write('Method: '+str(m.params.Method)+'\n')
    flog.write('Heuristics: '+str(m.params.Heuristics)+'\n')
    flog.write('SubMIPNodes: '+str(m.params.SubMIPNodes)+'\n')
    flog.write('MinRelNode : '+str(m.params.MinRelNodes)+'\n')
    flog.write('PumpPasses: '+str(m.params.PumpPasses)+'\n')
    flog.write('ZeroObjNodes: '+str(m.params.ZeroObjNodes)+'\n')
    flog.write('Cuts: '+str(m.params.Cuts)+'\n')
    flog.write('FlowCoverCuts: '+str(m.params.FlowCoverCuts)+'\n')
    flog.write('MIRCuts: '+str(m.params.MIRCuts)+'\n')
    flog.write('Presolve: '+str(m.params.Presolve)+'\n')
    flog.write('PrePasses: '+str(m.params.PrePasses)+'\n')
    flog.write('Aggregate: '+str(m.params.Aggregate)+'\n')
    flog.write('AggFill: '+str(m.params.AggFill)+'\n')
    flog.write('PreSparsif : '+str(m.params.PreSparsify)+'\n')
    flog.write('Symmetry: '+str(m.params.Symmetry)+'\n')
    flog.write('VarBranch: '+str(m.params.VarBranch)+'\n')
    flog.write('FeasibilityTol: '+str(m.params.FeasibilityTol)+'\n')
    flog.write('IntFeasTol: '+str(m.params.IntFeasTol)+'\n')
    flog.write('MarkowitzTol: '+str(m.params.MarkowitzTol)+'\n')
    flog.write('OptimalityTol: '+str(m.params.OptimalityTol)+'\n')
    flog.write("NumVars: "+ str(m.NumVars)+'\n')
    flog.write("NumConstrs: " + str(m.NumConstrs)+'\n')
    flog.write('\n')
    flog.write('\n')
    flog.write('\n')
    flog.close()
    
    m.write('E:\\TeachingMaterials\\BusinessAnalytics\\ProblemsExamples\\VerizonCellTowerGurobi\\thisModel'+str(thisYear)+'.mps')
    
    m.optimize()
    
    input_tower_cap = []
    for tower in towers:
        if x_cell_tower[tower].x == 1:
            input_tower_cap.append((tower,parameters['cell_tower_fixed_cap'] + parameters['cell_tower_add_cap'] * x_cell_cap_add[tower].x, thisYear))
            
    input_call_tower_pairs = []
    for (tower, call) in tower_call_pairs:
        #print '(tower, call):( ' + str(tower) + ',' + str(call) + ')'
        if x_cell_call_assign[(tower, call)].x == 1:
            input_call_tower_pairs.append((tower, call, thisYear))

    # Insert optimization results into database
    print 'starting DB work with results'
    cursor = cnx.cursor()        
    cursor.executemany("""INSERT INTO tboptresultscelltowercapadd (CellTowerID, CapacityCalls, Yr) VALUES (%s,%s,%s)""", input_tower_cap)
    print 'len(input_tower_cap): '+str(len(input_tower_cap))
    cnx.commit()
    cursor.close()
    
    cursor = cnx.cursor()
    cursor.executemany("""INSERT INTO tboptresults (CellTowerID, CallLocID, Yr) VALUES (%s,%s,%s)""", input_call_tower_pairs)
    print 'len(input_call_tower_pairs): '+str(len(input_call_tower_pairs))
    cnx.commit()
    cursor.close()
    
    # Call procedures to manipulate data and insert it into the tableau data files
    cursor = cnx.cursor()
    cursor.callproc('spOptResultsCallLocationToTableau',(thisYear,))
    cnx.commit()
    cursor.callproc('spOptResultsCellTowerToTableau',(thisYear,))
    cnx.commit()
    cursor.close()
     
    legacy_towers = []
    for tower in towers:
        if x_cell_tower[tower].x == 1:
            legacy_towers.append((tower, x_cell_cap_add[tower].x))
            
    #delete model
    print "NumVars: "+ str(m.NumVars) + "   NumConstrs: " + str(m.NumConstrs) 
    for i in reversed(range(len(m.getVars()))):
        m.remove(m.getVars()[i])
    del x_cell_tower
    del x_cell_cap_add
    del x_cell_call_assign
    m.update()
    print "NumVars: "+ str(m.NumVars) + "   NumConstrs: " + str(m.NumConstrs) 
    #m.remove(m.getConstrs()[0])
    for i in reversed(range(len(m.getConstrs()))):
        m.remove(m.getConstrs()[i])
    del x_leg_tower_constr
    del x_leg_tower_cap_add_constr
    m.update()
    print "NumVars: "+ str(m.NumVars) + "   NumConstrs: " + str(m.NumConstrs)
    
    m.reset()
    m.update()
    
    end_time = datetime.datetime.now()
    print "start time: ", start_time
    print 'end time: ', end_time
    print 'elapsed time:', end_time - start_time
            
    
cnx.close()

    