package sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.crypto.Data;

import org.json.JSONArray;
import org.json.JSONException;   
import org.json.JSONObject; 

public class basicStructure {

	public static void main(String args[]){
	}
}

class Condition{
	String l;
	String op;
	String r;
	private boolean rightIsColumn = false;
	
	public void rightIsColumn(){
		this.rightIsColumn = true;
	}
	
	private boolean compare(String value,String op,String value2){
		
		if(op.equals("==")){
			return value.equals(value2);
		}
		else if(op.equals("!=")){
			return !value.equals(value2);
		}
		else{ //只有数字才能比大小
		
			int val = Integer.parseInt(value);
			int val2 = Integer.parseInt(value2);
			if(op.equals(">>")){
				return val>val2;
			}
			else if(op.equals("<<")){
				return val<val2;
			}
			else if(op.equals("<=")){
				return val<=val2;
			}
			else if(op.equals(">=")){
				return val>=val2;
			}
			
		}
		return false;
	}
	
	public boolean match(JSONObject target) throws JSONException{
		if(!target.has(l))return true;
		String val1 = target.getString(l);
		String val2 = new String();
		if(rightIsColumn&&target.has(r)){
		   val2 = target.getString(r);
		}
		else if(!rightIsColumn){
			val2 = r;
		}
		return compare(val1,op,val2);
	}
	
	public String toString(){
		return l+" "+op+" "+r;
	}
	
	public Condition(String l,String op,String r){
		this.l = l; this.r = r; this.op = op;
	}
}


class CSet{ //condition set
	
	public List<Object> set = new ArrayList<Object>();
	
	static final public boolean OR = false;
	static final public boolean AND = true;
	
	private boolean type;
	
	public int size(){
		return set.size();
	}
	
	public Object popTheOnlyOne(){
		return set.size()==1?set.get(0):null;
	}
	
	public boolean match(JSONObject target){
		if(this.set.size()==0)return true;
		for(Object key:set){
			try{
				boolean isOk;
				if(key instanceof Condition){
					isOk = ((Condition)key).match(target);
				}
				else{
					isOk = ((CSet)key).match(target);
				}
				if(type&&!isOk)return false;
				if(!type&&isOk)return true;
			}catch(Exception e){
				System.out.println("错误:"+e.getMessage()+" 中断匹配");
				return false;
			}
		}
		return this.type;
	}
	
	public void put(Object condition){
		if(condition == null)return;
		if(condition instanceof CSet){
			int size = ((CSet)condition).size();
			if(size==0)return ;
			else if(size==1){
				set.add(((CSet)condition).popTheOnlyOne());
				return;
			}
		}
		set.add(condition);
	}
	
	public String toString(){
		String[] andb = {"{","}"},orb = {"[","]"};
		String[] bracket = this.type == CSet.AND?andb:orb;
		String result=bracket[0];
		for(Object key:set){
			if(key instanceof Condition){
				result += ((Condition)key).toString()+" ";
			}
			else{
				result += ((CSet)key).toString()+" ";
			}
		}
		return result+bracket[1];
	}
	
	CSet(boolean type){
		this.type = type;
	}
}

class Table{
	
	public String tableName;
	public JSONObject table;
	public JSONObject define;
	public CSet checkConditions = new CSet(CSet.AND);
	
	public String toString(){
		String result = null;
		if(table!=null){
			result = table.toString();
		}
		return result;
	}
	
	public Table join(Table target){
		JSONArray newData = new JSONArray();
		Table joined = new Table("tmp","{'data':[],'define':{},'unique':[],'check':{}}");
		try{
			JSONArray dataA = table.getJSONArray("data");
			JSONArray dataB = target.table.getJSONArray("data");
			for(int i=0;i<dataA.length();i++){
				
				JSONObject lineA = dataA.getJSONObject(i);
				JSONObject combine_head = new JSONObject();
				
				for(Iterator it = lineA.keys();it.hasNext();){
					String key = (String) it.next();
					combine_head.put(this.tableName+"."+key,lineA.get(key));
				}
				
				for(int k=0;k<dataB.length();k++){
					JSONObject lineB= dataB.getJSONObject(k);
					JSONObject combine = new JSONObject(combine_head.toString());
					for(Iterator it = lineB.keys();it.hasNext();){
						String key = (String)it.next();
						combine.put(target.tableName+"."+key,lineB.get(key));
					}
					newData.put(combine);
				}
			}
			joined.table.put("data", newData);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return joined;
	}
	
	private boolean checkUnique(JSONObject value,JSONArray data){
		try {
			JSONArray unique = table.getJSONArray("unique");
			List<String> checkList = new ArrayList<String>();
			for(int i=0;i<unique.length();i++){
				String key = unique.getString(i);
				if(value.has(key)){
					checkList.add(key);
				}
			}
			
			if(checkList.size()==0)return true;
			
			for(int i=0;i<data.length();i++){
				for(String key:checkList){
					if(value.get(key).equals(data.getJSONObject(i).get(key))){
						System.out.println(key+"中出现了重复:" + data.getJSONObject(i).get(key) +"\n"
								+ "而该键为Unique或primary key,拒绝数据更新");
						return false;
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	private boolean check(JSONObject value){
		boolean isOk = true;
		try{
			JSONObject check = table.getJSONObject("check");
			if(check.length()==0)return true;
			isOk = this.checkConditions.match(value);
		}catch(Exception e){
			
		}
		return isOk;
	}
	
	private void JudgeType(JSONObject value,String key,String type) throws JSONException{
		Object get = value.get(key);
		if(type.equals("int")){
			int test = (int)get;
		}
		else if(type.equals("string")){
			String test = (String)value.get(key);
		}
		else if(type.equals("bool")){
			boolean test = (boolean)value.get(key);
		}
	}
	
	//----------------------insert插入----------------------------
	public JSONObject insert(JSONObject value){
		
		String currentKey = "";
		try{
			
			for(Iterator i = define.keys();i.hasNext();){
				String key = (String)i.next();
				currentKey = key;
				String type = define.getString(key);
				JudgeType(value,key,type);
			}
			
			if(!checkUnique(value,table.getJSONArray("data"))){
				return null;
			}
			if(!check(value)){
				System.out.println("不符合约束!");
				return null;
			}
			
			table.getJSONArray("data").put(value);
			System.out.println("完成插入");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println(currentKey+"出现错误,插入失败");
			value = null;
		}
		return value;
	}
	//------------------------insert插入--------------------------
	
	//-------------------------update修改-------------------------
	public JSONArray update(JSONObject newValue,CSet Condition){
		JSONArray updated = new JSONArray();
		try{
			//修改的新值是否符合定义
			for(Iterator i = newValue.keys();i.hasNext();){
				String key = (String)i.next();
				String type = define.getString(key);
				JudgeType(newValue,key,type);
			}
			if(!check(newValue)){
				System.out.println("不符合约束!");
				return null;
			}

			JSONArray data = table.getJSONArray("data");
			JSONArray newdata = new JSONArray();
			for(int i=0;i<data.length();i++){
				String cloneStr = data.getJSONObject(i).toString();
				JSONObject target = new JSONObject(cloneStr);
				
				if(Condition!=null&&!Condition.match(target)){
					newdata.put(target);
					continue;
				}
				
				for(Iterator it = newValue.keys();it.hasNext();){
					String key = (String)it.next();
					if(!checkUnique(newValue,newdata)){
						return null;
					}
					target.put(key, newValue.get(key));
				}
				
				updated.put(target);
				newdata.put(target);
			}
			table.put("data", newdata);
			System.out.println("修改成功");
		}catch(Exception e){
			System.out.println("修改失败:"+e.getMessage());
		}
		return updated;
	}
	//-------------------------update修改-------------------------
	
	//-------------------------delete删除--------------------------
	
	public JSONArray delete(CSet Condition){
		JSONArray result = null;
		try {
			result = new JSONArray();
			JSONArray data = table.getJSONArray("data");
			JSONArray newdata = new JSONArray();
			for(int i=0;i<data.length();i++){
				JSONObject target = data.getJSONObject(i);
				if(Condition!=null&&!Condition.match(target)){
				    newdata.put(target);
				    continue;
				}
				result.put(target);
			}
			table.put("data", newdata);
			System.out.println("删除成功");
		} catch (JSONException e) {
			e.printStackTrace();
			System.out.println("删除失败");
		}
		
		return result;
	}
	
	//--------------------------delete------------------------------------
	
	//------------------------------query查找-----------------------------
	
	public JSONArray find(CSet Conditions){
		
		JSONArray result = null;
		try{
			JSONArray data = table.getJSONArray("data");
			result = new JSONArray();
			for(int i=0;i<data.length();i++){
				JSONObject target = (JSONObject) data.get(i);
				if(Conditions!=null&&!Conditions.match(target)){continue;}
				result.put(target);
			}	
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("查找失败");
		}
		System.out.println("完成查找");
		return result;
	}
	//-----------------------query查找---------------------------------------
	
	Table(String name,String JsonText){
		try {
			this.tableName = name;
			table = new JSONObject(JsonText);
			define = table.getJSONObject("define");
			JSONObject check = table.getJSONObject("check");
			if(check.length()>0){
				for(Iterator i = check.keys();i.hasNext();){
					String key = (String) i.next();
					String cond = check.getString(key);
					this.checkConditions.put(new Condition(key,cond.substring(0,2),cond.substring(2)));
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
//----------------------------------------------------视图索引
class View{
	public CSet condition;
	public ArrayList<String> ref;
	public ArrayList<String> target;
	
	View( ArrayList<String> ref,CSet condition,ArrayList<String> target){
		this.condition = condition;
		this.ref = ref;
		this.target = target;
	}
}
//----------------------------------------------------数据库操控
class DataBase{
	
	static public Map<String,Table> tables = new HashMap<String,Table>();
	
	static public Map<String,View> views = new HashMap<String,View>();
	
	static final String PATH = "./database/";
	
	static private String curDb = "default/";
	
	static public void createDb(String dbName){
		File f = new File(PATH+dbName);
		if(f.exists()){
			System.out.println("数据库已经存在");
		}
		else{
			try {
				f.mkdirs();
				System.out.println("数据库创建成功");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	static public void useDB(String dbName){
		File f = new File(PATH+dbName);
		writeFile(); //当前数据库打开的表写回
		if(f.exists()){
			curDb = dbName+"/";
			System.out.println("切换至数据库:"+dbName);
		}
		else{
			System.out.println(dbName+"数据库不存在");
		}
	}
	
	static public void dropDB(String dbName){
		File f = new File(PATH+dbName);
		if(f.exists()){
			String[] list = f.list();
			for(String key:list){
				dropTable(key.replaceAll(".txt", ""));
			}
			f.delete();
			System.out.println("数据库已经被删除,切换至数据库:default");
			curDb = "default";
		}else{
			System.out.println(dbName+"不存在");
		}
	}
	
	static public void createTable(String tableName,String define){
		File f = new File(PATH+curDb+tableName+".txt");
		if(f.exists()){
			System.out.println("已存在该表:"+tableName);
		}
		else{
			try {
				f.createNewFile();
				FileWriter fout = new FileWriter(f,false);
				fout.write(define);
				fout.flush();
				System.out.println(tableName + "表创建成功");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	static public boolean isOpen(String tableName){
		return tables.keySet().contains(tableName);
	}
	
	static public boolean openTable(String tableName){
		if(isOpen(tableName)){
			return true;
		}
		File f = new File(PATH+curDb+tableName+".txt");
		if(!f.exists()){
			System.out.println("当前数据库不存在该表:"+tableName);
			return false;
		}
		try {
			Scanner fin = new Scanner(f);
			String str = "";
			while(fin.hasNext()){
				str += fin.next();
			}
			if(str!=""){
				Table newTable = new Table(tableName,str);
				tables.put(tableName, newTable); 
			}
			else{
				System.out.println("表损坏");
			}
			return true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	static public void dropTable(String tableName){
		File f = new File(PATH+curDb+tableName+".txt");
		if(!f.exists()){
			System.out.println("当前数据库不存在该表:"+tableName);
			return ;
		}
		else{
			tables.remove(tableName);
			if(f.delete()){
				System.out.println("删除表"+tableName);
			}
		}
	}
	
	static public void createView(String name,ArrayList<String> ref,CSet Condition,ArrayList<String> target){
		if(name.equals(ref)){
			System.out.println("表名重复");
			return;
		}
		try{
			for(String key:ref){
				if(!openTable(key)){
				  System.out.println("索引表不存在 创建失败");
				  return;
				}
			}
			views.put(name, new View(ref,Condition,target));
			System.out.println("视图"+name+"创建成功");
		}catch(Exception e){}
	}
	
	
	static public void insert(String tableName,JSONObject value){
		if(openTable(tableName)){
			System.out.println(tables.get(tableName).insert(value));
		}
	}
	
	static public void update(String tableName,JSONObject newVal,CSet condition){
		if(openTable(tableName)){
			System.out.println(tables.get(tableName).update(newVal, condition));
		}
	}
	
	static public void delete(String tableName,CSet condition){
		if(openTable(tableName)){
			System.out.println(tables.get(tableName).delete(condition));
		}
	}
	
	static JSONArray filter(JSONArray result,ArrayList<String> target){
		
		 JSONArray filetered = new JSONArray();
		 try{
			 for(int i=0;i<result.length();i++){
				JSONObject newline = new JSONObject();
				JSONObject oldline = result.getJSONObject(i);
				for(String key:target){
					if(key.equals("*")){
						newline = result.getJSONObject(i);
						break;
					}
					if(oldline.has(key)){
						newline.put(key, oldline.get(key));
					}	
				}
				filetered.put(newline);
			 }
		 }catch(Exception e){e.printStackTrace();}
		 return filetered;
	}
	
	static public JSONArray select(ArrayList<String> tablesNames,CSet Conditions,ArrayList<String> target){
		JSONArray result = new JSONArray();
		try{
			
			ArrayList<Table> t = new ArrayList();
			Table targetT =null;
			for(String key:tablesNames){
				if(views.containsKey(key)){
					View v = views.get(key);
					Table tmp = new Table(key,"{'data':[],'define':{},'unique':[],'check':{}}");
					JSONArray viewData = DataBase.select(v.ref, v.condition, v.target);
					tmp.table.put("data", viewData);
					t.add(tmp);
				}
				else{
					if(openTable(key))t.add(tables.get(key));
					else return new JSONArray();       //表不存在就退出
				}
			}
			
			targetT = t.get(0);
			
			if(t.size()>=2){   //进行连接
				for(int i=1;i<t.size();i++){
					targetT = targetT.join(t.get(i));
				}
			}
			result = filter(targetT.find(Conditions),target);
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}
	
	static public void selectAndPrint(ArrayList<String> tablesNames,CSet Conditions,ArrayList<String> target){
		JSONArray result = select(tablesNames,Conditions,target);
		for(int i=0;i<result.length();i++){
			try {
				System.out.println(result.get(i).toString());
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}
	
	static public void  showTable(){
		File f = new File(PATH+curDb);
		String[] list = f.list();
		System.out.println("表单:");
		for(int i=0;i<list.length;i++){
			System.out.println(list[i].replaceAll(".txt", ""));
		}
	}
	
	static public void writeFile(){
		try{
			for(String key:tables.keySet()){
				File f = new File(PATH+curDb+key+".txt");
				FileWriter fout = new FileWriter(f,false);
				fout.write(tables.get(key).toString());
				fout.flush();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}


