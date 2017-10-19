package sql;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import sql.Node;
import sql.SimpleNode;

public class SemanticReader{
	
	private SimpleNode root;
	
	private String tableModel = "{'data':[],'define':{},'unique':[],'check':{}}";
	
	private void DFS(Node node){
		if(node!=null)visit(node);
		for(int i=0;i<node.jjtGetNumChildren();i++){
			DFS(node.jjtGetChild(i));
		}
	}
	
	private void visit(Node node){
		System.out.println(node.toString());
	}
	
	private SimpleNode getType(SimpleNode root){
		return (SimpleNode) root.jjtGetChild(0).jjtGetChild(0);
	}
	
	private SimpleNode getChild(SimpleNode n,int index){ //方便获取孩子节点 懒得转换
		return (SimpleNode)n.jjtGetChild(index);
	}
	
	public void Ccollector(SimpleNode n,JSONObject table)throws JSONException{
	
		//for createTable
		int childNum = n.jjtGetNumChildren();
		if(childNum == 0) return;
		
		//获取定义的列名与类型名字
		String column = getChild(n,0).name.toLowerCase();
		String type = getChild(n,1).name.toLowerCase();
		
		table.getJSONObject("define").put(column, type);
		
		if(childNum==4){
			 if(getChild(n,2).ntType().equals("P_Key")){ //获取主键
				table.put("primary_key",column);
				table.getJSONArray("unique").put(column);
			 }
			 else if(getChild(n,2).ntType().equals("Unique")){
				table.getJSONArray("unique").put(column);
			 }
			 else if(getChild(n,2).ntType().equals("Check")){
				 String op = getChild(getChild(n,2),0).name;
				 String value = getChild(getChild(getChild(n,2),1),0).name;
				 JSONObject check = new JSONObject();
				 table.getJSONObject("check").put(column, op+value);
			 }
			
			Ccollector((SimpleNode)n.jjtGetChild(3),table);
		}
		else if(childNum ==3){
			Ccollector((SimpleNode)n.jjtGetChild(2),table);
		}
	}
	
	public void goCreate(SimpleNode start){ //for create
		
		SimpleNode first = (SimpleNode) start.jjtGetChild(0);
		if(first.ntType().equals("DB")){
			String dbname = first.name;
			
			DataBase.createDb(dbname);
			
		}
		else if(first.ntType().equals("View")){
			
		     String viewName = getChild(first,0).name;
			 start = getChild(first,1);
			 
			 ArrayList<String> ref = new ArrayList<String>();
			 ArrayList<String> target = new ArrayList<String>();
			 
			 CSet Conditions = null;
			 int length = start.jjtGetNumChildren();
			 
			 Collector(getChild(start,1),ref);
			 Collector(getChild(start,0),target);
			 
			 System.out.println(target.toString());
			 System.out.println(ref.toString());
			 
			 if(length == 3){
				 Conditions = (CSet) getChild(start,2).Conditions;
			 }
			 DataBase.createView(viewName, ref, Conditions, target);
		}
		else{
			SimpleNode Definition = (SimpleNode)start.jjtGetChild(1);
			String tableName = first.name;
			
			try {
				JSONObject newTable = new JSONObject(this.tableModel);
				Ccollector(Definition,newTable);
				System.out.println(newTable.toString());
				
				DataBase.createTable(tableName, newTable.toString());
				
			} catch (JSONException e) {
				e.printStackTrace();
				System.out.println("创建表时出现错误");
			}
		}
	}
	
	public void valCollector(SimpleNode n,JSONObject newVal)throws Exception{
		// for insert update
		String col = getChild(n,0).name.toLowerCase();
		String value = getChild(getChild(n,1),0).name;
		String type = getChild(getChild(n,1),0).ntType();
		if(type.equals("Num")){
			newVal.put(col, Integer.parseInt(value));
		}
		else if(type.equals("Bool")){
			newVal.put(col, Boolean.parseBoolean(value));
		}
		else{
			value = value.replaceAll("'", "");
			newVal.put(col, value);
		}
		
		if(n.jjtGetNumChildren()==3){
			valCollector(getChild(n,2),newVal);
		}
	}
	
	public void valCollector2(SimpleNode n,JSONArray col,JSONArray val){
		for(int i=0;i<n.jjtGetNumChildren();i++){
			SimpleNode child = getChild(n,i);
			if(child.ntType().equals("Table"))continue;
			if(child.ntType().equals("Column"))col.put(child.name);
			if(child.ntType().equals("Value")){
				String type = getChild(child,0).ntType();
				String value = getChild(child,0).name;
				if(type.equals("Num")){
					val.put(Integer.parseInt(value));
				}
				else if(type.equals("Bool")){
					val.put(Boolean.parseBoolean(value));
				}
				else{
					val.put(value.replaceAll("'", ""));
				}
				continue;
			}
			valCollector2(child,col,val);
		}
	}
	
	public void goInsert(SimpleNode start){
		String tableName = getChild(start,0).name.toLowerCase();
		String next = getChild(start,1).ntType();
		JSONObject newVal = new JSONObject();
		try{
			if(next.equals("KEY_VALUE")){
				valCollector(getChild(start,1),newVal);
				DataBase.insert(tableName, newVal);
			}
			else{
				JSONArray col = new JSONArray(),val = new JSONArray();
				valCollector2(start,col,val);
				newVal = val.toJSONObject(col);
				DataBase.insert(tableName, newVal);
			}
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("插入新数据出现错误");
		}
	}
	
	public void Collector(SimpleNode node,ArrayList<String> target){
		//for select
		int num = node.jjtGetNumChildren();
		if(num==0)return;
		else{
			String name = getChild(node,0).name;
			if(name!=null)target.add(name);
			if(num!=1){
				Collector(getChild(node,1),target);
			}
		}
	}

	
	public void goSelect(SimpleNode start){
		
		 ArrayList<String> tablesNames = new ArrayList<String>();
		 ArrayList<String> target = new ArrayList<String>();
		 CSet Conditions = null;
		 
		 int length = start.jjtGetNumChildren();
		 Collector(getChild(start,0),target);
		 Collector(getChild(start,1),tablesNames);
		 
		 System.out.println(tablesNames.toString());
		 System.out.println(target.toString());
		 
		 if(length == 3){
			 Conditions = (CSet)getChild(start,2).Conditions;
			 System.out.println(Conditions.toString());
		 }
		 DataBase.selectAndPrint(tablesNames, Conditions, target);
	}

	public void goUpdate(SimpleNode start){
		String tableName = getChild(start,0).name;
		JSONObject newVal = new JSONObject();
		CSet condition = null;
		int num = start.jjtGetNumChildren();
		try {
			valCollector(getChild(start,1),newVal);
			if(num==3){
				condition = (CSet) getChild(start,2).Conditions;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		DataBase.update(tableName,newVal,condition);
	}
	
	public void goDelete(SimpleNode start){
		String tableName = getChild(start,0).name;
		SimpleNode follow = getChild(start,1);
		CSet condition = null;
		if(follow.jjtGetNumChildren()==1){
			condition = (CSet) getChild(follow,0).Conditions;
		}
		DataBase.delete(tableName, condition);
	}

	
	public void goDrop(SimpleNode start){
		String type = start.name;
		String target = getChild(start,0).name;
		System.out.println(type + " "+ target);
		if(type.equals("database")){
			if(!target.equals("default")){
				DataBase.dropDB(target);
			}else{
				System.out.println("默认数据库不能删除 当然你可以自己到文件夹里删掉");
			}
		}
		else if(type.equals("table")){
			System.out.println(target);
			DataBase.dropTable(target);
		}
	}
	
	public void goUse(SimpleNode start){
		String dbName = getChild(start,0).name;
		DataBase.useDB(dbName);
	}
	
	SemanticReader(SimpleNode root){
		this.root = root;
		//this.root.dump(""); //for debug
		SimpleNode startPoint = getType(this.root);
		String type = startPoint.ntType();
		if(type.equals("Exit")){
			
			System.out.println("退出数据库管理系统,谢谢您的使用");	
			DataBase.writeFile();//将内存中的数据写入文件	
			System.exit(0);;
		}
		else if(type.equals("Create")){
			goCreate(startPoint);
		}
		else if(type.equals("Insert")){
			goInsert(startPoint);
		}
		else if(type.equals("Drop")){
			goDrop(startPoint); 
		}
		else if(type.equals("Use")){
			goUse(startPoint);
		}
		else if(type.equals("Select")){
			goSelect(startPoint);
		}
		else if(type.equals("Delete")){
			goDelete(startPoint);
		}
		else if(type.equals("Update")){
			goUpdate(startPoint);
		}
		else if(type.equals("Show")){
			DataBase.showTable();
		}
		else{
			System.out.println("你输入的都是啥啊 老子识别不了");
		}
	}
}

