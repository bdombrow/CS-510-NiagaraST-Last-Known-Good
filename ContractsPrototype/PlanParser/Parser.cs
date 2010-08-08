namespace ContractsPrototype.PlanParser
{
    using System;
    using System.Linq;
    using System.Xml.Linq;
    using ContractsPrototype;

    public class Parser
    {
        public static Plan ParseXML(String pathToFile)
        {
            XElement xDoc = XElement.Load(pathToFile);

            int opCount = xDoc.Elements("operator").Count();
            Plan plan = new Plan();

            #region Operator parsing

            foreach (XElement op in xDoc.Elements("operator"))
            {
                string name = op.Attribute("id").Value;
                int inputArity = Int32.Parse(op.Attribute("inputArity").Value);
                int outputArity = Int32.Parse(op.Attribute("outputArity").Value);
                Operator opObj = new Operator(name, inputArity, outputArity);

                #region Contract offering parsing

                foreach (XElement contract in op.Elements("contract"))
                {
                    Offering offering = new Offering(inputArity, outputArity);

                    // input
                    //XElement input = contract.Element("input");
                    foreach (XElement input in contract.Elements("input"))
                    {
                        Scheme si = new Scheme();

                        foreach (XElement scheme in input.Elements("scheme"))
                        {
                            Template t = new Template();
                            foreach (XElement attribute in scheme.Elements("attribute"))
                            {
                                t.Add(attribute.Attribute("name").Value, ClassType.ToMark(attribute.Attribute("mark").Value));
                            }
                            si.Add(t);
                        }
                        offering.addInputScheme(Int32.Parse(input.Attribute("id").Value), si);
                    }

                    // output
                    foreach (XElement output in contract.Elements("output"))
                    {
                        Scheme so = new Scheme();

                        foreach (XElement scheme in output.Elements("scheme"))
                        {
                            Template t = new Template();
                            foreach (XElement attribute in scheme.Elements("attribute"))
                            {
                                t.Add(attribute.Attribute("name").Value, ClassType.ToMark(attribute.Attribute("mark").Value));
                            }
                            so.Add(t);
                        }
                        offering.addOutputScheme(Int32.Parse(output.Attribute("id").Value), so);
                    }

                    // feedback.input
                    foreach (XElement feedbackInput in contract.Elements("feedback.input"))
                    {
                        Scheme sfi = new Scheme();

                        foreach (XElement scheme in feedbackInput.Elements("scheme"))
                        {
                            if (scheme.Elements("any").Count() == 1)
                            {
                                offering.addFeedbackInputScheme(Int32.Parse(feedbackInput.Attribute("id").Value), Scheme.SchemeAsAny());
                                goto FI;
                            }
                            else if (scheme.IsEmpty)
                            {
                                offering.addFeedbackInputScheme(Int32.Parse(feedbackInput.Attribute("id").Value), sfi);
                                goto FI;
                            }
                            else
                            {
                                Template t = new Template();
                                foreach (XElement attribute in scheme.Elements("attribute"))
                                {
                                    t.Add(attribute.Attribute("name").Value, ClassType.ToMark(attribute.Attribute("mark").Value));
                                }
                                sfi.Add(t);
                            }
                            offering.addFeedbackInputScheme(Int32.Parse(feedbackInput.Attribute("id").Value), sfi);
                        }
                    FI:
                        continue;

                    }

                    // feedback output

                    foreach (XElement feedbackOutput in contract.Elements("feedback.output"))
                    {
                        Scheme sfo = new Scheme();

                        foreach (XElement scheme in feedbackOutput.Elements("scheme"))
                        {
                            if (scheme.Elements("any").Count() == 1)
                            {
                                offering.addFeedbackOutputScheme(Int32.Parse(feedbackOutput.Attribute("id").Value), Scheme.SchemeAsAny());
                                goto FO;
                            }
                            else if (scheme.IsEmpty)
                            {
                                offering.addFeedbackOutputScheme(Int32.Parse(feedbackOutput.Attribute("id").Value), sfo);
                                goto FO;
                            }
                            else
                            {
                                Template t = new Template();
                                foreach (XElement attribute in scheme.Elements("attribute"))
                                {
                                    t.Add(attribute.Attribute("name").Value, ClassType.ToMark(attribute.Attribute("mark").Value));
                                }
                                sfo.Add(t);
                            }
                            offering.addFeedbackOutputScheme(Int32.Parse(feedbackOutput.Attribute("id").Value), sfo);
                        }
                        FO:
                            continue;
                    }
                
                    opObj.AddOffering(offering);
                }
                #endregion
                //plan.Graph.AddNode(opObj.Name, opObj);
                plan.AddOperator(opObj);
            }
            #endregion

            #region DAG - add edges
            foreach (XElement op in xDoc.Elements("operator"))
            {
                string name = op.Attribute("id").Value;

                foreach (XElement attribute in op.Elements("input"))
                {
                    int inputId = Int32.Parse(attribute.Attribute("id").Value);
                    string inputName = attribute.Attribute("source").Value;

                    if (inputName != "")
                    {
                        var sourceNode = plan.Operators.Find(e => e.Name.Equals(inputName));
                        var destinationNode = plan.Operators.Find(e => e.Name.Equals(name));
                        destinationNode.AddAntecedent(sourceNode, inputId-1);
                        //var sourceNode = plan.Graph.Nodes[inputName];
                        //var destinationNode = plan.Graph.Nodes[name];
                        //plan.Graph.AddDirectedEdge(sourceNode, destinationNode);
                        //plan.Graph.AddDirectedEdge(destinationNode, sourceNode);
                    }
                }
            }
            #endregion

            return plan;
        }
    }
}
