// Standard C++ includes
#include <memory>
#include <vector>
#include <iostream>
#include <math.h>

#include "boost/algorithm/string.hpp"

// ROOT includes
#include <TTree.h>
#include <TLorentzVector.h>
#include <TPRegexp.h>

// CMSSW framework includes
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

// CMSSW data formats
#include "DataFormats/Math/interface/deltaR.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/Scouting/interface/ScoutingMuon.h"
#include "DataFormats/Scouting/interface/ScoutingParticle.h"
#include "DataFormats/Scouting/interface/ScoutingVertex.h"
#include "DataFormats/JetReco/interface/PFJet.h"
#include "JetMETCorrections/JetCorrector/interface/JetCorrector.h"
#include "SimDataFormats/PileupSummaryInfo/interface/PileupSummaryInfo.h"
#include "SimDataFormats/GeneratorProducts/interface/GenEventInfoProduct.h"
#include "SimDataFormats/GeneratorProducts/interface/GenLumiInfoHeader.h"

//Added for MET
#include "DataFormats/METReco/interface/PFMET.h"
#include "DataFormats/METReco/interface/PFMETCollection.h"
#include "DataFormats/PatCandidates/interface/Jet.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "DataFormats/PatCandidates/interface/MET.h"

//Added for offline electrons and muons
#include "DataFormats/PatCandidates/interface/Electron.h"
#include "DataFormats/PatCandidates/interface/Muon.h"
#include "DataFormats/EgammaCandidates/interface/GsfElectron.h"

// Adding Gen jets
#include "DataFormats/JetReco/interface/GenJet.h"

//Accessing Gen particles
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"

//Adding Reco vertices
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"

// Other relevant CMSSW includes
#include "CommonTools/UtilAlgos/interface/TFileService.h" 
#include "HLTrigger/HLTcore/interface/HLTConfigProvider.h"
#include "HLTrigger/HLTcore/interface/HLTPrescaleProvider.h"


#include <memory>
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Common/interface/TriggerNames.h"
#include "DataFormats/Common/interface/TriggerResults.h"
#include "DataFormats/HLTReco/interface/TriggerEvent.h"
#include "DataFormats/Scouting/interface/ScoutingElectron.h"
#include "DataFormats/Scouting/interface/ScoutingPhoton.h"
#include "DataFormats/Scouting/interface/ScoutingPFJet.h"

#include "DataFormats/Scouting/interface/ScoutingVertex.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"
#include "DataFormats/PatCandidates/interface/Muon.h"
#include "DataFormats/PatCandidates/interface/TriggerObjectStandAlone.h"
#include "DataFormats/PatCandidates/interface/PackedTriggerPrescales.h"
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"
#include "L1Trigger/L1TGlobal/interface/L1TGlobalUtil.h"
#include "DataFormats/L1TGlobal/interface/GlobalAlgBlk.h"
#include "HLTrigger/HLTcore/interface/TriggerExpressionData.h"
#include "HLTrigger/HLTcore/interface/TriggerExpressionEvaluator.h"
#include "HLTrigger/HLTcore/interface/TriggerExpressionParser.h"

#include <DataFormats/TrackReco/interface/TrackBase.h>
#include "DataFormats/ParticleFlowCandidate/interface/PFCandidate.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"

#include "DataFormats/Math/interface/libminifloat.h"

#include "DataFormats/Common/interface/ValueMap.h"
#include "DataFormats/Common/interface/View.h"

// Root include files
#include "TLorentzVector.h"
#include "TFile.h"
#include "TTree.h"
#include "TMath.h"
#include "TH2F.h"
#include "TCanvas.h"
#include "TVectorD.h"
#include "TMatrixD.h"
#include "TMatrixDSym.h"
#include "TMatrixDSymEigen.h"

// User include files

#include "fastjet/ClusterSequence.hh"
#include "fastjet/ClusterSequenceArea.hh"
#include "fastjet/contrib/Nsubjettiness.hh"
#include "fastjet/contrib/XConePlugin.hh"
#include "fastjet/contrib/SoftDrop.hh"
#include "fastjet/tools/Pruner.hh"
#include "fastjet/tools/Filter.hh"
#include "fastjet/contrib/RecursiveSoftDrop.hh"
#include "fastjet/contrib/EnergyCorrelator.hh"
#include "fastjet/JadePlugin.hh"
#include "fastjet/contrib/SoftKiller.hh"

#include "PhysicsTools/CandUtils/interface/EventShapeVariables.h"
#include "PhysicsTools/CandUtils/interface/Thrust.h"

#include "ScoutingNanoAOD_miniIso.h"

using namespace std;
using namespace fastjet;
using namespace fastjet::contrib;


//------------------------------------------------------------------------
 // the user information
 // 
 // To associate extra information to a PseudoJet, one first has to
 // create a class, derived from UserInfoBase, that contains
 // that information.
 //
 // In our simple example, we shall use 2 informations
 //  - the PDG id associated with the particle
 //  - the "vertex number" associated with the particle
 class PdgIdInfo : public PseudoJet::UserInfoBase{
 public:
   // default ctor
   //  - pdg_id        the PDG id of the particle
   PdgIdInfo(const int & pdg_id_in) :
     _pdg_id(pdg_id_in){}
  
   /// access to the PDG id
   int pdg_id() const { return _pdg_id;}
   
 protected:
   int _pdg_id;         // the associated pdg id
 };

//------------------------------------------------------------------------

class ScoutingNanoAOD_fromData : public edm::one::EDAnalyzer<edm::one::SharedResources, edm::one::WatchRuns, edm::one::WatchLuminosityBlocks> {
public:
  explicit ScoutingNanoAOD_fromData(const edm::ParameterSet&);
  ~ScoutingNanoAOD_fromData();
		
  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);
	
	
private:
  virtual void beginJob() override;
  virtual void analyze(const edm::Event&, const edm::EventSetup&) override;
  virtual void endJob() override;

  virtual void beginRun(edm::Run const&, edm::EventSetup const&) override;
  virtual void endRun(edm::Run const&, edm::EventSetup const&) override;
  virtual void beginLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) override;
  virtual void endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) override;
  int getCharge(int pdgId);
  bool jetID(const ScoutingPFJet &pfjet);
  bool jetIDoff(const pat::Jet &pfjet);

  //Scouting tokens
  const edm::InputTag triggerResultsTag;
  const edm::EDGetTokenT<edm::TriggerResults>             	triggerResultsToken;
  const edm::EDGetTokenT<std::vector<ScoutingMuon> >            muonsToken;
  const edm::EDGetTokenT<std::vector<ScoutingElectron> >  	electronsToken;
  const edm::EDGetTokenT<std::vector<ScoutingPhoton> >  	photonsToken;
  const edm::EDGetTokenT<std::vector<ScoutingParticle> >  	pfcandsToken;
  const edm::EDGetTokenT<std::vector<ScoutingPFJet> >  		pfjetsToken;
  const edm::EDGetTokenT<std::vector<ScoutingVertex> >  	verticesToken;

  const edm::EDGetTokenT<double> metPtToken;
  const edm::EDGetTokenT<double> metPhiToken;

  std::vector<std::string> triggerPathsVector;
  std::map<std::string, int> triggerPathsMap;
	
  // Generator-level information
  // Flags for the different types of triggers used in the analysis
  // For now we are interested in events passing either the single or double lepton triggers


  // Trigger information        
  bool runScouting = false;           
  //bool monitor;
  std::string label;

  HLTPrescaleProvider hltPSProv_;
  
  std::string hltProcess_; //name of HLT process, usually "HLT"

  edm::EDGetTokenT<edm::TriggerResults> triggerBits_;
  edm::Handle<edm::TriggerResults> triggerBits;

  std::vector<std::string>     l1Seeds_;
  std::vector<std::string>     hltSeeds_;
  std::vector<bool>            l1Result_;
  std::vector<int>             l1Prescale_;
  std::vector<bool>            hltResult_;
  std::vector<std::string>     hltResultName_;
  vector<double>            PSweights;

  UInt_t scouting_trig; 

  //Photon
  UInt_t n_pho;
  vector<Float16_t> 	     Photon_pt;
  vector<Float16_t>        Photon_eta;
  vector<Float16_t>        Photon_phi;
  vector<Float16_t>	       Photon_m;
  vector<Float16_t>	       Photon_sigmaietaieta;
  vector<Float16_t>	       Photon_HoE;
  vector<Float16_t>        Photon_ecaliso;
  vector<Float16_t>	       Photon_hcaliso;

  //Scouting Electron
  UInt_t n_ele;
  vector<Float16_t> 	     Electron_pt;
  vector<Float16_t>        Electron_eta;
  vector<Float16_t>        Electron_phi;
  vector<Float16_t>	       Electron_m;
  vector<Float16_t>        Electron_d0;
  vector<Float16_t>	       Electron_dz;
  vector<Float16_t>	       Electron_detain;
  vector<Float16_t>	       Electron_dphiin;
  vector<Float16_t>	       Electron_sigmaietaieta;
  vector<Float16_t>	       Electron_HoE;
  vector<Float16_t>	       Electron_ooEMOop;
  vector<Float16_t>	       Electron_mHits;
  vector<Float16_t>        Electron_charge;
  vector<Float16_t>        Electron_ecaliso;
  vector<Float16_t>	       Electron_hcaliso;
  vector<Float16_t>        Electron_trkiso;
  vector<Float16_t>        Electron_combinediso;
  vector<Float16_t>        Electron_miniIsoChg;
  vector<Float16_t>        Electron_miniIsoAll;
  vector<bool>             Electron_ID;


  //Muon
  UInt_t n_mu;
  vector<Float16_t>            Muon_pt;
  vector<Float16_t>            Muon_eta;
  vector<Float16_t>            Muon_phi;
  vector<Float16_t>            Muon_m;
  vector<Float16_t>            Muon_ecaliso;
  vector<Float16_t>            Muon_hcaliso;
  vector<Float16_t>            Muon_trkiso;
  vector<Float16_t>            Muon_chi2;
  vector<bool>                 Muon_isGlobalMuon;
  vector<bool>                 Muon_isTrackerMuon;
  vector<Float16_t>            Muon_ndof;
  vector<Float16_t>            Muon_charge;
  vector<Float16_t>            Muon_dxy;
  vector<Float16_t>            Muon_dz;
  vector<Float16_t>            Muon_nvalidmuon_hits;
  vector<Float16_t>            Muon_nvalidpixelhits;
  vector<Float16_t>            Muon_nmatchedstations;
  vector<Float16_t>            Muon_type;
  vector<Float16_t>            Muon_nvalidstriphits;
  vector<Float16_t>            Muon_trkqoverp;
  vector<Float16_t>            Muon_trklambda;
  vector<Float16_t>            Muon_trkpt;
  vector<Float16_t>            Muon_trkphi;
  vector<Float16_t>            Muon_trketa;
  vector<Float16_t>            Muon_trkqoverperror;
  vector<Float16_t>            Muon_trklambdaerror;
  vector<Float16_t>            Muon_trkpterror;
  vector<Float16_t>            Muon_trkphierror;
  vector<Float16_t>            Muon_trketaerror;
  vector<Float16_t>            Muon_trkdszerror;
  vector<Float16_t>            Muon_trkdsz;
  vector<Float16_t>            Muon_miniIsoChg;
  vector<Float16_t>            Muon_miniIsoAll;


  //PFJets
  UInt_t                       n_jet;
  UInt_t                       n_jetId;
  UInt_t                       n_jetoff;
  UInt_t                       n_jetIdoff;
  float                        ht;
  float                        htoff;
  bool                         passJetId;
  vector<Float16_t> 	         Jet_rawFactor;
  vector<Float16_t> 	         Jet_pt;
  vector<Float16_t>            Jet_eta;
  vector<Float16_t>            Jet_phi;
  vector<Float16_t>	           Jet_m;
  vector<Float16_t>	           Jet_area;
  vector<Float16_t>	           Jet_chargedHadronEnergy;
  vector<Float16_t>            Jet_neutralHadronEnergy;
  vector<Float16_t>	           Jet_photonEnergy;
  vector<Float16_t>	           Jet_electronEnergy;
  vector<Float16_t>	           Jet_muonEnergy;
  vector<Float16_t>	           Jet_HFHadronEnergy;
  vector<Float16_t>	           Jet_HFEMEnergy;
  vector<Float16_t>	           Jet_HOEnergy;
  vector<Float16_t>	           Jet_chargedHadronMultiplicity;
  vector<Float16_t>            Jet_neutralHadronMultiplicity;
  vector<Float16_t>	           Jet_photonMultiplicity;
  vector<Float16_t>	           Jet_electronMultiplicity;
  vector<Float16_t>	           Jet_muonMultiplicity;
  vector<Float16_t>	           Jet_HFHadronMultiplicity;
  vector<Float16_t>	           Jet_HFEMMultiplicity;
  vector<Float16_t> 	         Jet_csv;
  vector<Float16_t> 	         Jet_mvaDiscriminator;
  vector<Float16_t>  	         Jet_nConstituents;
  vector<bool>                 Jet_passId;

  // Scouting PFCand
  UInt_t                       n_pfcand;
  UInt_t                       n_pfMu;
  UInt_t                       n_pfEl;
  vector<Float16_t>            PFcand_pt;
  vector<Float16_t>            PFcand_eta;
  vector<Float16_t>            PFcand_phi;
  vector<Float16_t>            PFcand_m;
  vector<Float16_t>            PFcand_pdgid;
  vector<Float16_t>            PFcand_q;
  vector<Float16_t>            PFcand_vertex;
  vector<Float16_t>            FatJetPFCands_jetIdx;
  vector<Float16_t>            FatJetPFCands_pFCandsIdx;

  // Fatjets 
  UInt_t                       n_fatjet;
  vector<Float16_t>            FatJet_rawFactor;
  vector<Float16_t>            FatJet_area;
  vector<Float16_t>            FatJet_eta;
  //vector<Float16_t>            FatJet_n2b1;
  //vector<Float16_t>            FatJet_n3b1;
  vector<Float16_t>            FatJet_phi;
  vector<Float16_t>            FatJet_pt;
  //vector<Float16_t>            FatJet_tau1;
  //vector<Float16_t>            FatJet_tau2;
  //vector<Float16_t>            FatJet_tau3;
  //vector<Float16_t>            FatJet_tau4;
  //vector<Float16_t>            FatJet_tau21;
  //vector<Float16_t>            FatJet_tau32;
  vector<Float16_t>            FatJet_mass;
  //vector<Float16_t>            FatJet_msoftdrop;
  //vector<Float16_t>            FatJet_mtrim;
  vector<Float16_t>            FatJet_nconst;


  // Primary vertices
  UInt_t n_pvs;
  vector<Float16_t>            Vertex_x;
  vector<Float16_t>            Vertex_y;
  vector<Float16_t>            Vertex_z;
  vector<Float16_t>            Vertex_tracksSize;
  vector<Float16_t>            Vertex_chi2;
  vector<Float16_t>            Vertex_ndof;
  vector<Float16_t>            Vertex_isValidVtx;


  //Scouting MET
  double met_pt, met_phi;
  
  //reco vertices
  Int_t nPV_;        // number of reconsrtucted primary vertices
    
  // TTree carrying the event weight information
  TTree* tree;

  //Run and lumisection
  int run;
  int lumSec;
  int event_;

};

ScoutingNanoAOD_fromData::ScoutingNanoAOD_fromData(const edm::ParameterSet& iConfig): 
  
  //Scouting tokens
  muonsToken               (consumes<std::vector<ScoutingMuon> >             (iConfig.getParameter<edm::InputTag>("muons"))), 
  electronsToken           (consumes<std::vector<ScoutingElectron> >         (iConfig.getParameter<edm::InputTag>("electrons"))), 
  photonsToken             (consumes<std::vector<ScoutingPhoton> >           (iConfig.getParameter<edm::InputTag>("photons"))), 
  pfcandsToken             (consumes<std::vector<ScoutingParticle> >         (iConfig.getParameter<edm::InputTag>("pfcands"))), 
  pfjetsToken              (consumes<std::vector<ScoutingPFJet> >            (iConfig.getParameter<edm::InputTag>("pfjets"))), 
  verticesToken            (consumes<std::vector<ScoutingVertex> >           (iConfig.getParameter<edm::InputTag>("vertices"))),
   
  
  metPtToken               (consumes<double>                                    (iConfig.getParameter<edm::InputTag>("metPt"))),
  metPhiToken              (consumes<double>                                    (iConfig.getParameter<edm::InputTag>("metPhi"))),


  hltPSProv_(iConfig,consumesCollector(),*this), //it needs a referernce to the calling module for some reason, hence the *this   
  hltProcess_(iConfig.getParameter<std::string>("hltProcess")),
  triggerBits_(consumes<edm::TriggerResults>(iConfig.getParameter<edm::InputTag>("bits"))),
  l1Seeds_(iConfig.getParameter<std::vector<std::string> >("l1Seeds")),
  hltSeeds_(iConfig.getParameter<std::vector<std::string> >("hltSeeds"))


{
 // now do whatever initialization is needed
  usesResource("TFileService");


 // Access the TFileService
  edm::Service<TFileService> fs;

  // Create the TTree
  tree = fs->make<TTree>("Events"       , "tree");

  // Event weights 
  tree->Branch("lumSec"		                  ,&lumSec            ,"lumSec/i");
  tree->Branch("run"		                    ,&run                  ,"run/i");
  tree->Branch("event"		                    ,&event_                  ,"event/i");
  
  //scouting, offline triggers
  tree->Branch("scouting_trig"            	        ,&scouting_trig 			,"scounting_trig/i");

  //Scouting Electrons
  tree->Branch("nElectron"               	         ,&n_ele                        ,"nElectron/i");
  tree->Branch("Electron_pt"                    ,&Electron_pt                   );
  tree->Branch("Electron_eta"                   ,&Electron_eta 	                );
  tree->Branch("Electron_phi"                   ,&Electron_phi                  );
  tree->Branch("Electron_charge"                ,&Electron_charge               );
  tree->Branch("Electron_mass"            	        ,&Electron_m                    );
  tree->Branch("Electron_hoe"                   ,&Electron_HoE                  );
  tree->Branch("Electron_sieie"         ,&Electron_sigmaietaieta        );
  tree->Branch("Electron_dphiin"                ,&Electron_dphiin 	        );
  tree->Branch("Electron_detain"                ,&Electron_detain 	        );
  tree->Branch("Electron_mHits"                 ,&Electron_mHits 	        );
  tree->Branch("Electron_ooEMOop"               ,&Electron_ooEMOop              );            
  tree->Branch("Electron_trkiso"                 ,&Electron_trkiso 	        );
  tree->Branch("Electron_ecaliso"               ,&Electron_ecaliso              );
  tree->Branch("Electron_hcaliso"               ,&Electron_hcaliso              );
  tree->Branch("Electron_combinediso"               ,&Electron_combinediso   );
  tree->Branch("Electron_ID"               ,&Electron_ID   );
  tree->Branch("Electron_d0"               ,&Electron_d0              );
  tree->Branch("Electron_dz"               ,&Electron_dz              );
  tree->Branch("Electron_miniIsoChg"       ,&Electron_miniIsoChg              );
  tree->Branch("Electron_miniIsoAll"       ,&Electron_miniIsoAll              );


  //Scouting Photons
  tree->Branch("nPhotons"            	        ,&n_pho 			,"nPhotons/i");
  tree->Branch("Photon_pt"            	        ,&Photon_pt                     );
  tree->Branch("Photon_eta"            	        ,&Photon_eta                    );
  tree->Branch("Photon_phi"            	        ,&Photon_phi                    );	
  tree->Branch("Photon_mass"            	        ,&Photon_m 	                );
  tree->Branch("Photon_hcaliso"                 ,&Photon_hcaliso 		);
  tree->Branch("Photon_ecaliso"                 ,&Photon_ecaliso 		);
  tree->Branch("Photon_hoe"            	        ,&Photon_HoE                    );
  tree->Branch("Photon_sieie"           ,&Photon_sigmaietaieta	        );

  //Scouting muons
  tree->Branch("nMuons"            	        ,&n_mu 	                        ,"nMuons/i");
  tree->Branch("Muon_pt"                        ,&Muon_pt                       );
  tree->Branch("Muon_eta"                       ,&Muon_eta                      );
  tree->Branch("Muon_phi"                       ,&Muon_phi                      );
  tree->Branch("Muon_mass"                         ,&Muon_m                        );
  tree->Branch("Muon_ecaliso"                   ,&Muon_ecaliso                  );
  tree->Branch("Muon_hcaliso"                   ,&Muon_hcaliso                  );
  tree->Branch("Muon_tkIso"                    ,&Muon_trkiso                   );
  tree->Branch("Muon_chi2"                      ,&Muon_chi2                     );
  tree->Branch("Muon_isGlobal"              ,&Muon_isGlobalMuon             );
  tree->Branch("Muon_isTracker"             ,&Muon_isTrackerMuon            );
  tree->Branch("Muon_ndof"                      ,&Muon_ndof                     );
  tree->Branch("Muon_charge"                    ,&Muon_charge	                  );
  tree->Branch("Muon_dxy"                       ,&Muon_dxy                      );
  tree->Branch("Muon_dz"                        ,&Muon_dz                       );
  tree->Branch("Muon_nvalidmuon_hits"           ,&Muon_nvalidmuon_hits          );
  tree->Branch("Muon_validpixelhits"            ,&Muon_nvalidpixelhits          );
  tree->Branch("Muon_nmatchedstations"          ,&Muon_nmatchedstations         );
  tree->Branch("Muon_type"                      ,&Muon_type                     );
  tree->Branch("Muon_nvalidstriphits"           ,&Muon_nvalidstriphits          );
  tree->Branch("Muon_tkqoverp"                 ,&Muon_trkqoverp                );
  tree->Branch("Muon_tklambda"                 ,&Muon_trklambda                );
  tree->Branch("Muon_tkpt"                     ,&Muon_trkpt                    );
  tree->Branch("Muon_tkphi"                    ,&Muon_trkphi                   );
  tree->Branch("Muon_tketa"                    ,&Muon_trketa                   );
  tree->Branch("Muon_tkqoverperror"            ,&Muon_trkqoverperror           );
  tree->Branch("Muon_tklambdaerror"            ,&Muon_trklambdaerror           );
  tree->Branch("Muon_tkpterror"                ,&Muon_trkpterror               );
  tree->Branch("Muon_tkphierror"               ,&Muon_trkphierror              );
  tree->Branch("Muon_tketaerror"               ,&Muon_trketaerror              );
  tree->Branch("Muon_tkdszerror"               ,&Muon_trkdszerror              );
  tree->Branch("Muon_tkdsz"                    ,&Muon_trkdsz                   );
  tree->Branch("Muon_miniIsoChg"               ,&Muon_miniIsoChg               );
  tree->Branch("Muon_miniIsoAll"               ,&Muon_miniIsoAll               );


  //Scouting AK4 PFJets
  tree->Branch("nJet"            	        ,&n_jet                         ,"nJet/i");
  tree->Branch("nJetId"            	        ,&n_jetId                       ,"nJetId/i");
  tree->Branch("Jet_pt"            	        ,&Jet_pt                        );
  tree->Branch("Jet_eta"            	        ,&Jet_eta                       );
  tree->Branch("Jet_phi"            	        ,&Jet_phi                       );
  tree->Branch("Jet_mass"            	        ,&Jet_m                         );
  tree->Branch("Jet_area"            	        ,&Jet_area                      );
  tree->Branch("Jet_chargedHadronEnergy"        ,&Jet_chargedHadronEnergy       );
  tree->Branch("Jet_neutralHadronEnergy"        ,&Jet_neutralHadronEnergy       );
  tree->Branch("Jet_photonEnergy"               ,&Jet_photonEnergy 	        );
  tree->Branch("Jet_electronEnergy"             ,&Jet_electronEnergy            );
  tree->Branch("Jet_muonEnergy"    	        ,&Jet_muonEnergy                );
  tree->Branch("Jet_HFHadronEnergy"             ,&Jet_HFHadronEnergy            );
  tree->Branch("Jet_HFEMEnergy"                 ,&Jet_HFEMEnergy                );
  tree->Branch("Jet_HOEnergy"                   ,&Jet_HOEnergy                  );
  tree->Branch("Jet_chargedHadronMultiplicity"  ,&Jet_chargedHadronMultiplicity );
  tree->Branch("Jet_neutralHadronMultiplicity"  ,&Jet_neutralHadronMultiplicity );
  tree->Branch("Jet_photonMultiplicity"         ,&Jet_photonMultiplicity        );
  tree->Branch("Jet_electronMultiplicity"       ,&Jet_electronMultiplicity      );
  tree->Branch("Jet_muonMultiplicity"           ,&Jet_muonMultiplicity          );
  tree->Branch("Jet_HFHadronMultiplicity"       ,&Jet_HFHadronMultiplicity      );
  tree->Branch("Jet_HFEMMultiplicity"           ,&Jet_HFEMMultiplicity          );
  tree->Branch("Jet_csv"            	        ,&Jet_csv                       );
  tree->Branch("Jet_mvaDiscriminator"           ,&Jet_mvaDiscriminator          );
  tree->Branch("Jet_nConstituents"              ,&Jet_nConstituents             );
  tree->Branch("Jet_passId"                     ,&Jet_passId                    );

  //Scouting AK8 PFJets
  tree->Branch("nFatJet"                        ,&n_fatjet                      ,"nFatJet/i");
  //tree->Branch("FatJet_area"                    ,&FatJet_area                   );
  tree->Branch("FatJet_eta"                     ,&FatJet_eta                    );
  //tree->Branch("FatJet_n2b1"                    ,&FatJet_n2b1                   );
  //tree->Branch("FatJet_n3b1"                    ,&FatJet_n3b1                   );
  tree->Branch("FatJet_phi"                     ,&FatJet_phi                    );
  tree->Branch("FatJet_pt"                      ,&FatJet_pt                     );
  //tree->Branch("FatJet_tau1"                    ,&FatJet_tau1                   );
  //tree->Branch("FatJet_tau2"                    ,&FatJet_tau2                   );
  //tree->Branch("FatJet_tau3"                    ,&FatJet_tau3                   );
  //tree->Branch("FatJet_tau4"                    ,&FatJet_tau4                   );
  //tree->Branch("FatJet_tau21"                   ,&FatJet_tau21                  );
  //tree->Branch("FatJet_tau32"                   ,&FatJet_tau32                  );
  tree->Branch("FatJet_mass"                    ,&FatJet_mass                   );
  //tree->Branch("FatJet_msoftdrop"               ,&FatJet_msoftdrop              );
  //tree->Branch("FatJet_mtrim"                   ,&FatJet_mtrim                  );
  tree->Branch("FatJet_nconst"                  ,&FatJet_nconst                 );

  //Scouting PF Candidates
  tree->Branch("nPFCands"            	        ,&n_pfcand 		        ,"nPFCands/i");	
  tree->Branch("nPFMuons"            	        ,&n_pfMu 		        ,"nPFMuons/i");	
  tree->Branch("nPFElectrons"            	        ,&n_pfEl 		        ,"nPFElectrons/i");	
  tree->Branch("PFCands_pt"        	        ,&PFcand_pt 		        );
  tree->Branch("PFCands_eta"            	        ,&PFcand_eta 	                );
  tree->Branch("PFCands_phi"            	        ,&PFcand_phi		        );
  tree->Branch("PFCands_mass"            	        ,&PFcand_m 		        );
  tree->Branch("PFCands_pdgId"                   ,&PFcand_pdgid                  );
  tree->Branch("PFCands_charge"                       ,&PFcand_q                      );
  tree->Branch("PFCands_vertex"                  ,&PFcand_vertex                 );

  //CZZ: added jet indices for Scouting PF candidates
  tree->Branch("FatJetPFCands_jetIdx"                   ,&FatJetPFCands_jetIdx 	                );
  tree->Branch("FatJetPFCands_pFCandsIdx"                   ,&FatJetPFCands_pFCandsIdx 	                );

  //Primary vertices 
  tree->Branch("nPVs"            	        ,&n_pvs                         ,"nPVs/i");	
  tree->Branch("PV_x"        	        ,&Vertex_x  		        );
  tree->Branch("PV_y"                       ,&Vertex_y   	                );
  tree->Branch("PV_z"                       ,&Vertex_z  		        );
  tree->Branch("PV_tracksSize"              ,&Vertex_tracksSize 	        );
  tree->Branch("PV_chi2"                    ,&Vertex_chi2	                );
  tree->Branch("PV_ndof"                    ,&Vertex_ndof	                );
  tree->Branch("PV_isValidVtx"              ,&Vertex_isValidVtx 	        );

  //Other variables
  tree->Branch("ht"                             ,&ht                            );

  //CZZ: added MET collections
  tree->Branch("MET_pt",&met_pt);
  tree->Branch("MET_phi",&met_phi);

}


ScoutingNanoAOD_fromData::~ScoutingNanoAOD_fromData() {
}

void ScoutingNanoAOD_fromData::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup) {
  using namespace edm;
  using namespace std;
  using namespace reco;
  
  Handle<vector<ScoutingElectron> > electronsH;
  Handle<vector<ScoutingMuon> > muonsH;
  Handle<vector<ScoutingPhoton> > photonsH;
  Handle<vector<ScoutingPFJet> > pfjetsH;

  Handle<vector<ScoutingParticle> > pfcandsH;
  Handle<vector<ScoutingVertex> > verticesH;
  
  Handle<double> metPt;
  Handle<double> metPhi;


  if(auto handle = iEvent.getHandle(pfcandsToken)){
    runScouting = true;
  }

  if(runScouting){
    iEvent.getByToken(electronsToken, electronsH);
    iEvent.getByToken(muonsToken, muonsH);
    iEvent.getByToken(photonsToken, photonsH);
    iEvent.getByToken(pfjetsToken, pfjetsH);
    iEvent.getByToken(pfcandsToken, pfcandsH);
    iEvent.getByToken(verticesToken, verticesH);
    iEvent.getByToken(metPtToken, metPt);
    iEvent.getByToken(metPhiToken, metPhi);

  }
  

  
  run = iEvent.eventAuxiliary().run();
  event_ = iEvent.eventAuxiliary().event();
  lumSec = iEvent.eventAuxiliary().luminosityBlock();  

  // Which triggers fired
  hltResult_.clear();
  hltResultName_.clear();

  iEvent.getByToken(triggerBits_, triggerBits);

  const edm::TriggerNames &names = iEvent.triggerNames(*triggerBits);
  scouting_trig=0; 
  for(size_t j = 0; j < hltSeeds_.size(); j++){
        TPRegexp pattern(hltSeeds_[j]);
        TPRegexp pattern1("DST_HT410_PFScouting_v");
    for (unsigned int i = 0, n = triggerBits->size(); i < n; ++i) {                                                          
      const std::string& hltbitName = names.triggerName(i);
      std::string hltpathName = hltbitName;
      bool hltpassFinal = triggerBits->accept(i);
        if( 
          TString(hltpathName).Contains(pattern1) and hltpassFinal)
          {
          scouting_trig=1;
          }  
      }
  }


  // *
  // Electrons here
  // *

  Electron_pt.clear();
  Electron_eta.clear();
  Electron_phi.clear();
  Electron_m.clear();
  Electron_d0.clear();
  Electron_dz.clear();
  Electron_detain.clear();
  Electron_dphiin.clear();
  Electron_sigmaietaieta.clear();
  Electron_HoE.clear();
  Electron_ooEMOop.clear();
  Electron_mHits.clear();
  Electron_charge.clear();
  Electron_ecaliso.clear();
  Electron_hcaliso.clear();
  Electron_trkiso.clear();
  Electron_combinediso.clear();
  Electron_ID.clear();
  n_ele = 0;

  vector<ScoutingParticle> PFcands;
  PFcands.clear();


  if(runScouting){
    for (auto electrons_iter = electronsH->begin(); electrons_iter != electronsH->end(); ++electrons_iter) 
      {
        Electron_pt.push_back(electrons_iter->pt());
        Electron_eta.push_back(electrons_iter->eta());
        Electron_phi.push_back(electrons_iter->phi());	
        Electron_m.push_back(electrons_iter->m());
        Electron_detain.push_back(electrons_iter->dEtaIn());
        Electron_dphiin.push_back(electrons_iter->dPhiIn());
        Electron_sigmaietaieta.push_back(electrons_iter->sigmaIetaIeta());
        Electron_HoE.push_back(electrons_iter->hOverE());	
        Electron_ooEMOop.push_back(electrons_iter->ooEMOop());
        Electron_mHits.push_back(electrons_iter->missingHits());
        Electron_charge.push_back(electrons_iter->charge());
        Electron_trkiso.push_back(electrons_iter->trackIso());
        Electron_ecaliso.push_back(electrons_iter->ecalIso());
        Electron_hcaliso.push_back(electrons_iter->hcalIso());
        Electron_d0.push_back(electrons_iter->d0());
        Electron_dz.push_back(electrons_iter->dz());
        n_ele++;

        //Notice: here pushing back the electrons in the PFcands vector (for scouting electromns are not stored as PFcands, but as a separate collection)
        ScoutingParticle tmp(electrons_iter->pt(),electrons_iter->eta(),electrons_iter->phi(),electrons_iter->m(),(-11)*electrons_iter->charge(),0);
        PFcands.push_back(tmp);
        TLorentzVector electron_p4 = TLorentzVector();
        electron_p4.SetPtEtaPhiM(electrons_iter->pt(), electrons_iter->eta(), electrons_iter->phi(), electrons_iter->m());
        float combinediso; 
        bool electronID = false;
        if(abs(electrons_iter->eta())<1.479){
        combinediso = (electrons_iter->trackIso() + std::max(0.f,electrons_iter->ecalIso() -1.f) + electrons_iter->hcalIso()) / electron_p4.Et();
          electronID = 
          (fabs(electrons_iter->dEtaIn()) < 0.007)
          & (fabs(electrons_iter->dPhiIn()) < 0.15)
          & (electrons_iter->sigmaIetaIeta() < 0.01)
          & (electrons_iter->hOverE() < 0.12)
          & (fabs(electrons_iter->d0()) < 0.02)
          & (fabs(electrons_iter->dz()) < 0.2)
          & (electrons_iter->ooEMOop() < 0.05)
          & (combinediso < 0.15);
        }else{
        combinediso = (electrons_iter->trackIso() + electrons_iter->ecalIso()  + electrons_iter->hcalIso()) / electron_p4.Et();
          electronID = 
          (fabs(electrons_iter->dEtaIn()) < 0.009)
          & (fabs(electrons_iter->dPhiIn()) < 0.10)
          & (electrons_iter->sigmaIetaIeta() < 0.03)
          & (electrons_iter->hOverE() < 0.10)
          & (fabs(electrons_iter->d0()) < 0.02)
          & (fabs(electrons_iter->dz()) < 0.2)
          & (electrons_iter->ooEMOop() < 0.05)
          & (combinediso < 0.15);
        }
        Electron_combinediso.push_back(combinediso);
        Electron_ID.push_back(electronID);
    }
  }


  // *
  // Photons here
  // *
  Photon_pt.clear();
  Photon_eta.clear();
  Photon_phi.clear();
  Photon_m.clear();
  Photon_sigmaietaieta.clear();
  Photon_HoE.clear();
  Photon_ecaliso.clear();
  Photon_hcaliso.clear();
  n_pho = 0;

  if(runScouting){
    for (auto photons_iter = photonsH->begin(); photons_iter != photonsH->end(); ++photons_iter) {
      Photon_pt.push_back(photons_iter->pt());
      Photon_eta.push_back(photons_iter->eta());
      Photon_phi.push_back(photons_iter->phi());
      Photon_m.push_back(photons_iter->m());
      Photon_sigmaietaieta.push_back(photons_iter->sigmaIetaIeta());
      Photon_HoE.push_back(photons_iter->hOverE());
      Photon_ecaliso.push_back(photons_iter->ecalIso());
      Photon_hcaliso.push_back(photons_iter->hcalIso());
      
      n_pho++;
    }
  }

  // *
  // Primary vertices
  // * 
  n_pvs = 0;
  Vertex_x.clear();
  Vertex_y.clear();
  Vertex_z.clear();
  Vertex_tracksSize.clear();
  Vertex_chi2.clear();
  Vertex_ndof.clear();
  Vertex_isValidVtx.clear();
  if(runScouting){
  for (auto vertices_iter = verticesH->begin(); vertices_iter != verticesH->end(); ++vertices_iter) {
        Vertex_x.push_back( vertices_iter->x() );
        Vertex_y.push_back( vertices_iter->y() );
        Vertex_z.push_back( vertices_iter->z() );
        Vertex_tracksSize.push_back( vertices_iter->tracksSize() );
        Vertex_chi2.push_back( vertices_iter->chi2() );
        Vertex_ndof.push_back( vertices_iter->ndof() );
        Vertex_isValidVtx.push_back( vertices_iter->isValidVtx() );
        n_pvs++;
    }
  }



  // * 
  // Particle Flow candidates 
  // *

  PFcand_pt.clear();
  PFcand_eta.clear();
  PFcand_phi.clear();
  PFcand_m.clear();
  PFcand_pdgid.clear();
  PFcand_q.clear();
  PFcand_vertex.clear();
  FatJetPFCands_jetIdx.clear();
  FatJetPFCands_pFCandsIdx.clear();

  vector<PseudoJet> fj_part;
  n_pfcand = 0;
  n_pfMu =0;
  n_pfEl =0;

  
  if(runScouting){

    for (auto pfcands_iter = pfcandsH->begin(); pfcands_iter != pfcandsH->end(); ++pfcands_iter) {
      ScoutingParticle tmp(MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter->pt())),MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter->eta())),MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter->phi())),pfcands_iter->m(),pfcands_iter->pdgId(),pfcands_iter->vertex());
    
      PFcands.push_back(tmp);
    }
    

    //sort PFcands according to pT
    struct {
      bool operator()(ScoutingParticle a, ScoutingParticle b) const { return a.pt() > b.pt(); }
    } custompT;

    std::sort(PFcands.begin(), PFcands.end(), custompT);


    for(auto & pfcands_iter : PFcands ){ //fills PFcand track info
      
      if (pfcands_iter.pt() < 0.5) continue;
      if (abs(pfcands_iter.eta()) >= 2.4 ) continue;

      PFcand_pt.push_back(MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter.pt())));
      PFcand_eta.push_back(MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter.eta())));
      PFcand_phi.push_back(MiniFloatConverter::float16to32(MiniFloatConverter::float32to16(pfcands_iter.phi())));
      if(abs(pfcands_iter.pdgId()) == 13){
        n_pfMu ++;
      }
      if(abs(pfcands_iter.pdgId()) == 11){
        n_pfEl ++;
      }
    
      PFcand_m.push_back(pfcands_iter.m());
      PFcand_pdgid.push_back(pfcands_iter.pdgId());
      PFcand_q.push_back(getCharge(pfcands_iter.pdgId()));
      PFcand_vertex.push_back(pfcands_iter.vertex());

      // For clustering fat jets
      PseudoJet temp_jet = PseudoJet(0, 0, 0, 0);
      temp_jet.reset_PtYPhiM(pfcands_iter.pt(), pfcands_iter.eta(), pfcands_iter.phi(), pfcands_iter.m());
      temp_jet.set_user_index(n_pfcand);
      temp_jet.set_user_info(new PdgIdInfo(pfcands_iter.pdgId()));
      fj_part.push_back(temp_jet);
      
      n_pfcand++;
    }

  }


  // 
  // Scouting muons   
  //

  Muon_pt.clear();
  Muon_eta.clear();
  Muon_phi.clear();
  Muon_m.clear();
  Muon_ecaliso.clear();
  Muon_hcaliso.clear();
  Muon_trkiso.clear();
  Muon_chi2.clear();
  Muon_isGlobalMuon.clear();
  Muon_isTrackerMuon.clear();
  Muon_ndof.clear();
  Muon_charge.clear();
  Muon_dxy.clear();
  Muon_dz.clear();
  Muon_nvalidmuon_hits.clear();
  Muon_nvalidpixelhits.clear();
  Muon_nmatchedstations.clear();
  Muon_type.clear();
  Muon_nvalidstriphits.clear();
  Muon_trkqoverp.clear();
  Muon_trklambda.clear();
  Muon_trkpt.clear();
  Muon_trkphi.clear();
  Muon_trketa.clear();
  Muon_trkqoverperror.clear();
  Muon_trklambdaerror.clear();
  Muon_trkpterror.clear();
  Muon_trkphierror.clear();
  Muon_trketaerror.clear();
  Muon_trkdszerror.clear();
  Muon_trkdsz.clear();
  n_mu=0;  
  
  if(runScouting){
  for (auto muons_iter = muonsH->begin(); muons_iter != muonsH->end(); ++muons_iter) {
 	  Muon_pt.push_back(muons_iter->pt()); 
   	Muon_eta.push_back(muons_iter->eta());
   	Muon_phi.push_back(muons_iter->phi());
   	Muon_m.push_back(muons_iter->m());
   	Muon_ecaliso.push_back(muons_iter->ecalIso());
   	Muon_hcaliso.push_back(muons_iter->hcalIso());
   	Muon_trkiso.push_back(muons_iter->trackIso());
   	Muon_chi2.push_back(muons_iter->chi2());
   	Muon_ndof.push_back(muons_iter->ndof());
   	Muon_charge.push_back(muons_iter->charge());
   	Muon_dxy.push_back(muons_iter->dxy());
   	Muon_dz.push_back(muons_iter->dz());
   	Muon_nvalidmuon_hits.push_back(muons_iter->nValidMuonHits());
   	Muon_nvalidpixelhits.push_back(muons_iter->nValidPixelHits());
   	Muon_nmatchedstations.push_back(muons_iter->nMatchedStations());
    Muon_type.push_back(muons_iter->type());
    Muon_nvalidstriphits.push_back(muons_iter->nValidStripHits());
    Muon_trkqoverp.push_back(muons_iter->trk_qoverp());
    Muon_trklambda.push_back(muons_iter->trk_lambda());
    Muon_trkpt.push_back(muons_iter->trk_pt());
    Muon_trkphi.push_back(muons_iter->trk_phi());
    Muon_trketa.push_back(muons_iter->trk_eta());
    Muon_trkqoverperror.push_back(muons_iter->dxyError());
    Muon_trklambdaerror.push_back(muons_iter->dzError());
    Muon_trkpterror.push_back(muons_iter->trk_qoverpError());
    Muon_trkphierror.push_back(muons_iter->trk_lambdaError());
    Muon_trketaerror.push_back(muons_iter->trk_phiError());
    Muon_trkdszerror.push_back(muons_iter->trk_dsz());
    Muon_trkdsz.push_back(muons_iter->trk_dszError());
    Muon_isGlobalMuon.push_back(muons_iter->isGlobalMuon());
    Muon_isTrackerMuon.push_back(muons_iter->isTrackerMuon());
    n_mu++;
  }
}

  //* loop over electrons to compute to compute the mini-isolation
  if(runScouting){
    for (auto electrons_iter = electronsH->begin(); electrons_iter != electronsH->end(); ++electrons_iter) 
      {
        TLorentzVector electron_p4 = TLorentzVector();
        electron_p4.SetPtEtaPhiM(electrons_iter->pt(), electrons_iter->eta(), electrons_iter->phi(), electrons_iter->m());
        reco::Candidate::PolarLorentzVector electron_p4_reco(electron_p4.Pt(), electron_p4.Eta(), electron_p4.Phi(), electron_p4.M());
        pat::PFIsolation ele_miniiso =  getMiniPFIsolationScout(PFcands, electron_p4_reco);
        auto chg = ele_miniiso.chargedHadronIso();
        auto neu = ele_miniiso.neutralHadronIso();
        auto pho = ele_miniiso.photonIso();
        float scale = 1.0 / electrons_iter->pt();
        Electron_miniIsoChg.push_back(scale * chg);
        Electron_miniIsoAll.push_back(scale * (chg + std::max(0.0, static_cast<double>(neu + pho) )));
        
    }
  }

  // * Loop over Muons and compute mini-isolaton
  if(runScouting){
    for (auto muons_iter = muonsH->begin(); muons_iter != muonsH->end(); ++muons_iter) 
      {
        TLorentzVector muon_p4 = TLorentzVector();
        muon_p4.SetPtEtaPhiM(muons_iter->pt(), muons_iter->eta(), muons_iter->phi(), muons_iter->m());
        reco::Candidate::PolarLorentzVector muon_p4_reco(muon_p4.Pt(), muon_p4.Eta(), muon_p4.Phi(), muon_p4.M());
        pat::PFIsolation mu_miniiso =  getMiniPFIsolationScout(PFcands, muon_p4_reco);
        auto chg = mu_miniiso.chargedHadronIso();
        auto neu = mu_miniiso.neutralHadronIso();
        auto pho = mu_miniiso.photonIso();
        float scale = 1.0 / muons_iter->pt();
        Muon_miniIsoChg.push_back(scale * chg);
        Muon_miniIsoAll.push_back(scale * (chg + std::max(0.0, static_cast<double>(neu + pho) )));
        
    }
  }

  // * 
  // AK4 Jets 
  // * 
  Jet_pt.clear();
  Jet_eta.clear();
  Jet_phi.clear();
  Jet_m.clear();
  Jet_area.clear();
  Jet_chargedHadronEnergy.clear();
  Jet_neutralHadronEnergy.clear();
  Jet_photonEnergy.clear();
  Jet_electronEnergy.clear();
  Jet_muonEnergy.clear();
  Jet_HFHadronEnergy.clear();
  Jet_HFEMEnergy.clear();
  Jet_HOEnergy.clear();
  Jet_chargedHadronMultiplicity.clear();
  Jet_neutralHadronMultiplicity.clear();
  Jet_photonMultiplicity.clear();
  Jet_electronMultiplicity.clear();
  Jet_muonMultiplicity.clear();
  Jet_HFHadronMultiplicity.clear();
  Jet_HFEMMultiplicity.clear();
  Jet_csv.clear();
  Jet_mvaDiscriminator.clear();
  Jet_nConstituents.clear();
  Jet_passId.clear();
  n_jet = 0;
  n_jetId = 0;
  ht = 0;
  htoff = 0;
  passJetId = false;

  if(runScouting){

       for (auto pfjet = pfjetsH->begin(); pfjet != pfjetsH->end(); ++pfjet) {


        Jet_pt .push_back( pfjet->pt() );
        Jet_eta.push_back( pfjet->eta());
        Jet_phi.push_back( pfjet->phi());
        Jet_m  .push_back( pfjet->m()  );
        Jet_area.push_back( pfjet->jetArea());

        Jet_chargedHadronEnergy.push_back( pfjet->chargedHadronEnergy());
        Jet_neutralHadronEnergy.push_back( pfjet->neutralHadronEnergy());
        Jet_photonEnergy       .push_back( pfjet->photonEnergy()       );
        Jet_electronEnergy     .push_back( pfjet->electronEnergy()     );
        Jet_muonEnergy         .push_back( pfjet->muonEnergy()     );
        Jet_HFHadronEnergy     .push_back( pfjet->HFHadronEnergy() );
        Jet_HFEMEnergy         .push_back( pfjet->HFEMEnergy()     );
        Jet_HOEnergy           .push_back( pfjet->HOEnergy()       );
        
        Jet_chargedHadronMultiplicity.push_back( pfjet->chargedHadronMultiplicity());
        Jet_neutralHadronMultiplicity.push_back( pfjet->neutralHadronMultiplicity());
        Jet_photonMultiplicity       .push_back( pfjet->photonMultiplicity()       );
        Jet_electronMultiplicity     .push_back( pfjet->electronMultiplicity()     );
        Jet_muonMultiplicity         .push_back( pfjet->muonMultiplicity()         );
        Jet_HFHadronMultiplicity     .push_back( pfjet->HFHadronMultiplicity()     );
        Jet_HFEMMultiplicity         .push_back( pfjet->HFEMMultiplicity()         );

        Jet_csv             .push_back( pfjet->csv() );
        Jet_mvaDiscriminator.push_back( pfjet->mvaDiscriminator()    );
        Jet_nConstituents   .push_back( pfjet->constituents().size() );
        
        n_jet++;

        passJetId = jetID(*pfjet);
        Jet_passId.push_back( passJetId );

        // apply jet ID 
        if ( passJetId == false ) continue; 
        if (pfjet->pt() < 30){continue;}//raise pt threshold for HT calculation 
        ht += pfjet->pt() ; 
        n_jetId++ ; 

      }
  
  }
  

  // * 
  // FatJets 
  // *

  //here introduce AK definition
  JetDefinition ak8_def = JetDefinition(antikt_algorithm, 0.8);
  fastjet::GhostedAreaSpec area_spec(5.0,1,0.01);
  fastjet::AreaDefinition area_def(fastjet::active_area, area_spec);

  //here add for scouting AK8 jets
  FatJet_eta .clear();
  FatJet_phi .clear();
  FatJet_pt  .clear();
  FatJet_mass.clear();
  FatJet_nconst.clear();
  n_fatjet = 0;


  if(runScouting){

    ClusterSequenceArea ak8_cs(fj_part, ak8_def, area_def);
    vector<PseudoJet> ak8_jets = sorted_by_pt(ak8_cs.inclusive_jets(100)); //pt min cut at 100 GeV

    for (auto &j: ak8_jets) {
      FatJet_eta.push_back(j.eta());
      FatJet_phi.push_back(j.phi_std());
      FatJet_pt .push_back(j.pt());
      FatJet_mass.push_back(j.m());
      FatJet_nconst.push_back(j.constituents().size());
      n_fatjet++;
    }

      unsigned int n_pfcand_tot = 0;
      for (auto & pfcands_iter : PFcands ) {
        if (pfcands_iter.pt() < 1.) continue;
        if (abs(pfcands_iter.eta()) >= 2.4 ) continue;    
        int tmpidx = -1;
        int ak8count = 0;
        for (auto &j: ak8_jets) {
          for (auto &k: j.constituents()){
            if ((UInt_t)k.user_index() == n_pfcand_tot){
              tmpidx = ak8count;
              ak8count++;
              break;
            }
          }
          if (tmpidx>-1){
            FatJetPFCands_jetIdx.push_back(tmpidx);
            FatJetPFCands_pFCandsIdx.push_back(n_pfcand_tot);
            break;
          }else{
            ak8count++;
          }
        }
        n_pfcand_tot++;
      }
    
  }


 //Adding met info
 met_pt = -1;
 met_phi = -1;

 if (runScouting){
  met_pt = *metPt;
  met_phi = *metPhi;
 }
 

tree->Fill();	

}



void ScoutingNanoAOD_fromData::beginJob() {
  
}

void ScoutingNanoAOD_fromData::endJob() {
}

void ScoutingNanoAOD_fromData::beginRun(edm::Run const& iRun, edm::EventSetup const& iSetup) {

  //we need to initalise the menu each run (menu can and will change on run boundaries)           
  //for L1
  bool changed=false;
  hltPSProv_.init(iRun,iSetup,hltProcess_,changed);
  const l1t::L1TGlobalUtil& l1GtUtils = hltPSProv_.l1tGlobalUtil();
  std::cout <<"l1 menu "<<l1GtUtils.gtTriggerMenuName()<<" version "<<l1GtUtils.gtTriggerMenuVersion()<<" comment "<<std::endl;
  std::cout <<"hlt name "<<hltPSProv_.hltConfigProvider().tableName()<<std::endl;

}

void ScoutingNanoAOD_fromData::endRun(edm::Run const&, edm::EventSetup const&) {
}

void ScoutingNanoAOD_fromData::beginLuminosityBlock(edm::LuminosityBlock const& iLumi, edm::EventSetup const&) {
}

void ScoutingNanoAOD_fromData::endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) {
}

int ScoutingNanoAOD_fromData::getCharge(int pdgId) {
  // following workbook 
  if      (abs(pdgId) == 11 ) return 1; // electron
  else if (abs(pdgId) == 13 ) return 1; // muon
  else if (abs(pdgId) == 211) return 1; // pion
  return 0;
  // 130 = KLong - neutral hadron 
  // 22 = photon 
  // 1 = HF hadron, where HF means forward calo
  // 2 = HF em particle, where HF means forward calo
}
//bool ScoutingNanoAOD::jetIDoff(const reco::PFJet &pfjet){
bool ScoutingNanoAOD_fromData::jetIDoff(const pat::Jet &pfjet){
// https://twiki.cern.ch/twiki/bin/view/CMS/JetID13TeVRun2018
    //moved HT cut
    TLorentzVector jet; 
    jet.SetPtEtaPhiM(pfjet.pt(), pfjet.eta(), pfjet.phi(), pfjet.mass() );
    
    float NHF  = pfjet.neutralHadronEnergy()/jet.E();
    float NEMF = pfjet.photonEnergy()/jet.E();
    float CHF  = pfjet.chargedHadronEnergy()/jet.E();
    float MUF  = pfjet.muonEnergy()/jet.E();
    float CEMF = pfjet.electronEnergy()/jet.E();
    float NumConst = pfjet.chargedHadronMultiplicity()+pfjet.neutralHadronMultiplicity()+pfjet.photonMultiplicity() + pfjet.electronMultiplicity() + pfjet.muonMultiplicity() + pfjet.HFHadronMultiplicity() + pfjet.HFEMMultiplicity();
    float CHM      = pfjet.chargedHadronMultiplicity() +pfjet.electronMultiplicity() + pfjet.muonMultiplicity(); 
    bool passID = (abs(pfjet.eta())<=2.6 && CEMF<0.8 && CHM>0 && CHF>0 && NumConst>1 && NEMF<0.9 && MUF <0.8 && NHF < 0.9 );

    return passID;
}
bool ScoutingNanoAOD_fromData::jetID(const ScoutingPFJet &pfjet){
// https://twiki.cern.ch/twiki/bin/view/CMS/JetID13TeVRun2018
    //moved HT cut
    TLorentzVector jet; 
    jet.SetPtEtaPhiM(pfjet.pt(), pfjet.eta(), pfjet.phi(), pfjet.m() );
    
    float NHF  = pfjet.neutralHadronEnergy()/jet.E();
    float NEMF = pfjet.photonEnergy()/jet.E();
    float CHF  = pfjet.chargedHadronEnergy()/jet.E();
    float MUF  = pfjet.muonEnergy()/jet.E();
    float CEMF = pfjet.electronEnergy()/jet.E();
    float NumConst = pfjet.chargedHadronMultiplicity()+pfjet.neutralHadronMultiplicity()+pfjet.photonMultiplicity() + pfjet.electronMultiplicity() + pfjet.muonMultiplicity() + pfjet.HFHadronMultiplicity() + pfjet.HFEMMultiplicity();
    float CHM      = pfjet.chargedHadronMultiplicity() +pfjet.electronMultiplicity() + pfjet.muonMultiplicity(); 
    bool passID = (abs(pfjet.eta())<=2.6 && CEMF<0.8 && CHM>0 && CHF>0 && NumConst>1 && NEMF<0.9 && MUF <0.8 && NHF < 0.9 );

    return passID;
}

void ScoutingNanoAOD_fromData::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;
  desc.setUnknown();
  descriptions.addDefault(desc);
}

DEFINE_FWK_MODULE(ScoutingNanoAOD_fromData);
