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
#include "SimDataFormats/PileupSummaryInfo/interface/PileupSummaryInfo.h"
#include "SimDataFormats/GeneratorProducts/interface/GenEventInfoProduct.h"
#include "SimDataFormats/GeneratorProducts/interface/GenLumiInfoHeader.h"

//Added for offline jets
#include "DataFormats/JetReco/interface/PFJet.h"
#include "DataFormats/JetReco/interface/PFJetCollection.h"
#include "JetMETCorrections/JetCorrector/interface/JetCorrector.h"

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
#include "DataFormats/EgammaCandidates/interface/GsfElectronFwd.h"
#include "DataFormats/EgammaCandidates/interface/GsfElectronCore.h"
#include "DataFormats/MuonReco/interface/Muon.h"

// Adding Gen jets
#include "DataFormats/JetReco/interface/GenJet.h"

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


/*
  Defines a function to compute MiniIsolation given a 4-vector and a collection
  of packed PF candidates.

  Mini-Isolation reference: https://hypernews.cern.ch/HyperNews/CMS/get/susy/1991.html

*/

//Implementation of MiniIsolation for Scouting (following https://github.com/cms-sw/cmssw/blob/0cbc4b00b1664496105480e9f561a29ba7c9a68b/PhysicsTools/PatUtils/interface/MiniIsolation.h)
float miniIsoDrScout(const reco::Candidate::PolarLorentzVector &p4, float mindr, float maxdr, float kt_scale) {
    return std::max(mindr, std::min(maxdr, float(kt_scale / p4.pt())));
  };

pat::PFIsolation getMiniPFIsolationScout(const vector<ScoutingParticle> pfcands,
                                const reco::Candidate::PolarLorentzVector &p4,
                                float mindr = 0.05,
                                float maxdr = 0.2,
                                float kt_scale = 10.0,
                                float ptthresh = 0.5,
                                float deadcone_ch = 0.0001,
                                float deadcone_ph = 0.01,
                                float deadcone_nh = 0.01
                                ) {
    
    float chiso = 0, nhiso = 0, phiso = 0 , puiso = 0;
    float drcut = miniIsoDrScout(p4, mindr, maxdr, kt_scale);
    for (const auto &pc : pfcands) {
      float dr2 = deltaR2(p4, pc);
      if (dr2 > drcut * drcut)
        continue;
      float pt = pc.pt();
      int id = pc.pdgId();
      if (std::abs(id) == 211) {
        if (dr2 > deadcone_ch * deadcone_ch) {
          // if charged hadron and from primary vertex, add to charged hadron isolation
          chiso += pt;
        } 
      }
      // if neutral hadron, add to neutral hadron isolation
      if (std::abs(id) == 130 && pt > ptthresh && dr2 > deadcone_nh * deadcone_nh)
        nhiso += pt;
      // if photon, add to photon isolation
      if (std::abs(id) == 22 && pt > ptthresh && dr2 > deadcone_ph * deadcone_ph)
        phiso += pt;
    }

    return pat::PFIsolation(chiso, nhiso, phiso, puiso);
  };
