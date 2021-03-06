/* Generated By:JJTree: Do not edit this line. SimpleNode.java Version 4.3 */
/* JavaCCOptions:MULTI=false,NODE_USES_PARSER=false,VISITOR=false,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package sql;

public
class SimpleNode implements Node {

  protected Node parent;
  protected Node[] children;
  protected int id;
  protected Object value;
  protected MyNewGrammar parser;
  
  public String name;
  public Object Conditions;
  
  public void setName(String name){
	  this.name = name;
  }
  
  public void setConditions(Object Conditions){
	  this.Conditions = Conditions;
  }

  public SimpleNode(int i) {
    id = i;
  }

  public SimpleNode(MyNewGrammar p, int i) {
    this(i);
    parser = p;
  }

  public void jjtOpen() {
  }

  public void jjtClose() {
  }

  public void jjtSetParent(Node n) { parent = n; }
  public Node jjtGetParent() { return parent; }

  public void jjtAddChild(Node n, int i) {
    if (children == null) {
      children = new Node[i + 1];
    } else if (i >= children.length) {
      Node c[] = new Node[i + 1];
      System.arraycopy(children, 0, c, 0, children.length);
      children = c;
    }
    children[i] = n;
  }

  public Node jjtGetChild(int i) {
    return children[i];
  }

  public int jjtGetNumChildren() {
    return (children == null) ? 0 : children.length;
  }

  public void jjtSetValue(Object value) { this.value = value; }
  public Object jjtGetValue() { return value; }

  /* You can override these two methods in subclasses of SimpleNode to
     customize the way the node appears when the tree is dumped.  If
     your output uses more than one line you should override
     toString(String), otherwise overriding toString() is probably all
     you need to do. */

  public String toString(){ 
	  String suffix = this.name!=null?name:"";
	  String cond="";
	  if(this.Conditions!=null){
		  cond = "\n"+this.Conditions.toString();
	  }
	  return MyNewGrammarTreeConstants.jjtNodeName[id]+" "+suffix+cond; 
  }
  
  public String ntType(){
	  return MyNewGrammarTreeConstants.jjtNodeName[id];
  }
  
  public String toString(String prefix) { return prefix + toString(); }

  /* Override this method if you want to customize how the node dumps
     out its children. */

  public void dump(String prefix) {
    System.out.println(toString(prefix));
    if (children != null) {
      for (int i = 0; i < children.length; ++i) {
        SimpleNode n = (SimpleNode)children[i];
        if (n != null) {
          n.dump(prefix + " ");
        }
      }
    }
  }
}
/* JavaCC - OriginalChecksum=9749e263c620d082d28a32e38121a747 (do not edit this line) */
